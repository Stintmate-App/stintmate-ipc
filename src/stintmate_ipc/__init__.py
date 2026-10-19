"""Stintmate IPC v2: the Python reference implementation of SPEC.md.

Quick start::

    from stintmate_ipc import Endpoint, Client, PeerInfo, KeyFile, Layout, pipe_path

    key = KeyFile(r"C:\\Users\\me\\Documents\\Stintmate\\ipc\\key", create=True)
    ep = Endpoint(PeerInfo("pitwall", name="Pitwall", version="1.0"), key)

    @ep.method("pitwall.echo")
    def echo(ctx, params):
        return params

    laps = ep.event_channel("pitwall.lap_completed")
    ep.start()

    c = Client(pipe_path("pitwall"), PeerInfo("connector-examplesim"), key)
    c.connect()
    c.call("pitwall.echo", {"x": 1})
    c.subscribe("pitwall.lap_completed", print)
"""

from .decl import ChannelDecl, Declarations, MethodDecl
from .errors import (CallTimeout, ConnectionLost, EndpointInUse, HandshakeError, IpcError,
                     KeyFileError, NotConnected, PipeBusy, PipeClosed, PipeError, PipeNotFound,
                     ProtocolError, RemoteError)
from .handshake import PeerInfo
from .keyfile import KeyFile, default_key_path, ensure_key, read_key
from .layout import Layout
from .naming import (RegistryEntry, find_workspace, list_endpoints, pipe_path, pipe_user,
                     registry_dir, short_name)
from .node import (Channel, Client, ClientSubscription, Context, Endpoint, EventChannel,
                   IdempotencyCache, StateChannel, StreamChannel, SubscribeRequest, connect)
from .peer import Event, PendingCall, Peer, Sample, Subscription
from .wire import PROTOCOL_VERSION

__version__ = "0.1.0"

__all__ = [
    "PROTOCOL_VERSION", "CallTimeout", "Channel", "ChannelDecl", "Client",
    "ClientSubscription", "ConnectionLost", "Context", "Declarations", "Endpoint",
    "EndpointInUse", "Event", "EventChannel", "HandshakeError", "IdempotencyCache",
    "IpcError", "KeyFile", "KeyFileError", "Layout", "MethodDecl", "NotConnected", "Peer",
    "PeerInfo", "PendingCall", "PipeBusy", "PipeClosed", "PipeError", "PipeNotFound",
    "ProtocolError", "RegistryEntry", "RemoteError", "Sample", "StateChannel",
    "StreamChannel", "SubscribeRequest", "Subscription", "connect", "default_key_path",
    "ensure_key", "find_workspace", "list_endpoints", "pipe_path", "pipe_user", "read_key",
    "registry_dir", "short_name",
]
