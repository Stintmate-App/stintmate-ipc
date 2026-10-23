"""Stintmate IPC v2 for Python: a binding of the Rust implementation (PRY-085).

The client side (T6): ``Client``, ``connect``, ``Peer``, ``PendingCall``, subscriptions,
keys, naming, layouts and the exceptions. The endpoint side (T7): ``Endpoint``, ``Context``, the channels, ``decl``.
"""

import atexit as _atexit

from stintmate_ipc import _native, check, decl, keyfile
from stintmate_ipc._errors import (CallCancelled, CallTimeout, ConnectionLost, EndpointInUse,
                                   HandshakeError, IpcError, KeyFileError, NotConnected,
                                   PipeBusy, PipeClosed, PipeError, PipeNotFound,
                                   ProtocolError, RemoteError)
from stintmate_ipc._naming import RegistryEntry, list_endpoints
from stintmate_ipc._native import (PROTOCOL_VERSION, Client, ClientSubscription, Context,
                                   Endpoint, Event, EventChannel, StateChannel,
                                   StreamChannel, SubscribeRequest, SubscriptionView,
                                   KeyFile, Layout, PeerInfo, PendingCall, Peer, Sample,
                                   Subscription, connect, default_key_path, ensure_key,
                                   find_workspace, pipe_path, pipe_user, read_key,
                                   registry_dir, short_name, spec_revision, version)

__version__ = version()
_ = (check, decl, keyfile)

# Library threads must not attach to a finalizing interpreter.
_atexit.register(_native._shutdown)

__all__ = [
    "PROTOCOL_VERSION", "CallCancelled", "CallTimeout", "Client", "ClientSubscription",
    "ConnectionLost", "Context", "Endpoint", "EndpointInUse", "Event", "EventChannel", "HandshakeError", "IpcError", "KeyFile",
    "KeyFileError", "Layout", "NotConnected", "Peer", "PeerInfo", "PendingCall", "PipeBusy",
    "PipeClosed", "PipeError", "PipeNotFound", "ProtocolError", "RegistryEntry",
    "RemoteError", "Sample", "StateChannel", "StreamChannel", "SubscribeRequest",
    "Subscription", "SubscriptionView", "connect", "default_key_path", "ensure_key",
    "find_workspace", "list_endpoints", "pipe_path", "pipe_user", "read_key", "registry_dir",
    "short_name", "spec_revision", "version",
]
