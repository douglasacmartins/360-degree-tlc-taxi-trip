"""
OpenLineage Telemetry and Run Tracking.

This module provides a context-managed wrapper around the OpenLineage client 
to easily track data pipeline executions, state transitions, and data lineage. 

It ensures strict state transition validation (e.g., START -> RUNNING -> COMPLETE) 
and automatically captures and emits error facets with stack traces if an 
exception occurs during the pipeline's execution.

Key Components:
    - VALID_TRANSITIONS: A directed graph defining permissible state changes 
      to ensure data integrity in lineage tracking.
    - OpenLineageRun: A context manager that instantiates a run, manages its 
      lifecycle, handles error interception, and emits standard OpenLineage events.
"""


import traceback
from datetime import datetime, timezone
from types import MappingProxyType, TracebackType
from typing import ClassVar, Type
from uuid import uuid7

from openlineage.client import client, facet, run

VALID_TRANSITIONS: MappingProxyType[run.RunState, set[run.RunState]] = MappingProxyType({
    run.RunState.OTHER: {run.RunState.START},
    run.RunState.START: {run.RunState.RUNNING, run.RunState.COMPLETE, run.RunState.FAIL, run.RunState.ABORT},
    run.RunState.RUNNING: {run.RunState.COMPLETE, run.RunState.FAIL, run.RunState.ABORT},
    run.RunState.COMPLETE: set(),
    run.RunState.FAIL: set(),
    run.RunState.ABORT: set()
})

class OpenLineageRun:
    """
    A context manager for tracking an OpenLineage Job Run.

    This class manages the lifecycle of a pipeline run, ensuring that OpenLineage 
    events (START, COMPLETE, FAIL, etc.) are emitted accurately. When used as a 
    context manager, it automatically intercepts unhandled exceptions, attaches 
    the stack trace to an ErrorMessageRunFacet, and sets the run state to FAIL.
    If the block executes successfully, the state is set to COMPLETE.

    Attributes:
        namespace (str): The logical namespace of the job (e.g., 'production-etl').
        name (str): The name of the job.
        inputs (list[run.Dataset]): Datasets consumed by this run.
        outputs (list[run.Dataset]): Datasets produced by this run.
    
    Example:
        OpenLineageRun.set_client(client)
        OpenLineageRun.set_producer("https://github.com/my-repo/my-pipeline")
        
        with OpenLineageRun(namespace="sales", name="daily_aggregation") as ol_run:
            ol_run.start()
            ol_run.inputs.append(Dataset(namespace="sales", name="raw_transactions"))
            # ... execute job logic ...
            ol_run.outputs.append(Dataset(namespace="sales", name="agg_transactions"))
    """

    client: ClassVar[client.OpenLineageClient]
    producer: ClassVar[str] = ""

    _state: run.RunState
    namespace: str
    name: str
    inputs: list[run.Dataset]
    outputs: list[run.Dataset]
    run: run.Run
    job: run.Job

    def __init__(self, namespace: str, name: str):
        self.namespace = namespace
        self.name = name
        self.inputs = list()
        self.outputs = list()
        self.run = run.Run(runId=str(uuid7()))
        self.job = run.Job(namespace=namespace, name=name)
        self._state = run.RunState.OTHER

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type: Type[BaseException], exc_value: BaseException, exc_tb: TracebackType):
        if exc_type is not None:
            trace = [
                item.strip()
                for sublist in [tb.split("\n") for tb in traceback.format_tb(exc_tb)]
                for item in sublist
                if item and "~" not in item
            ]
            self.run.facets["errorMessage"] = facet.ErrorMessageRunFacet(
                message=str(exc_value),
                stackTrace="\n".join(trace) if len(trace) > 1 else trace[0],
                programmingLanguage="python"
            )
            self.state = run.RunState.FAIL
        else:
            self.state = run.RunState.COMPLETE

    @classmethod
    def set_client(cls, client: client.OpenLineageClient):
        cls.client = client

    @classmethod
    def set_producer(cls, producer: str):
        cls.producer = producer

    def start(self):
        self.state = run.RunState.START

    @property
    def state(self) -> run.RunState:
        return self._state

    @state.setter
    def state(self, state: run.RunState):
        if state not in VALID_TRANSITIONS[self._state]:
            raise ValueError(
                f"Invalid transition: Cannot transition from '{self._state.value}' to '{state.value}'"
            )
        self._state = state
        self._emit(self._get_event(state))

    @classmethod
    def _emit(cls, event: client.Event):
        cls.client.emit(event)

    def _get_event(self, state: run.RunState) -> client.Event:
        return run.RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=self.producer,
            run=self.run,
            job=self.job,
            inputs=self.inputs,
            outputs=self.outputs
        )