"""Core theoretical engine for DLM-Gate (zero external dependencies)."""

from src.core.dlm import DLMLabel
from src.core.clocks import VectorClock, GCounter
from src.core.spanning_tree import TreeMessage, TreeNode, SpanningTreeResult, build_default_depth2_tree, run_bcast_convergecast
from src.core.gate_fsm import DEFAULT_SCHEMAS, DeclassRequest, DeclassDecision, GateFSM

__all__ = [
    "DLMLabel",
    "VectorClock",
    "GCounter",
    "TreeMessage",
    "TreeNode",
    "SpanningTreeResult",
    "build_default_depth2_tree",
    "run_bcast_convergecast",
    "DEFAULT_SCHEMAS",
    "DeclassRequest",
    "DeclassDecision",
    "GateFSM",
]
