// P/Invoke declarations of the Stintmate IPC C ABI (include/stintmate_ipc.h).
// The statuses are the STINTMATE_IPC_* constants; payloads are MessagePack bytes; a buffer
// returned by the library is released with stintmate_ipc_free_buffer.
using System;
using System.Runtime.InteropServices;
using System.Text;

namespace Stintmate.Ipc
{
    [StructLayout(LayoutKind.Sequential)]
    public struct Buffer
    {
        public IntPtr Ptr;
        public UIntPtr Len;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct KeySource
    {
        public UIntPtr StructSize;
        public IntPtr Path;
        public IntPtr Key;
        public UIntPtr KeyLen;
        public int Create;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct PeerInfo
    {
        public IntPtr App;
        public IntPtr Name;
        public IntPtr Version;
        public IntPtr Process;
        public IntPtr Meta;
        public UIntPtr MetaLen;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct Delivery
    {
        public int Kind;
        public uint Flags;
        public IntPtr Channel;
        public UIntPtr ChannelLen;
        public ulong Seq;
        public long TsNs;
        public int HasTs;
        public IntPtr Data;
        public UIntPtr Len;
    }

    // Runs on the connection's callback lane: keep it short. Keep the delegate alive
    // (a field) for as long as the subscription exists.
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void DeliveryCallback(IntPtr userData, ref Delivery delivery);

    public static class Native
    {
        public const int Ok = 0;
        public const int ERemote = -13;
        const string Lib = "stintmate_ipc";

        [DllImport(Lib)] public static extern IntPtr stintmate_ipc_version();
        [DllImport(Lib)] public static extern UIntPtr stintmate_ipc_last_error_message(byte[] buf, UIntPtr len);
        [DllImport(Lib)] public static extern void stintmate_ipc_free_buffer(Buffer buf);

        [DllImport(Lib)] public static extern int stintmate_ipc_client_new(byte[] pipe, ref PeerInfo info, ref KeySource key, IntPtr opts, out IntPtr client);
        [DllImport(Lib)] public static extern int stintmate_ipc_client_start(IntPtr client);
        [DllImport(Lib)] public static extern int stintmate_ipc_client_wait_connected(IntPtr client, long timeoutMs, IntPtr peerOut);
        [DllImport(Lib)] public static extern void stintmate_ipc_client_free(IntPtr client);
        [DllImport(Lib)] public static extern int stintmate_ipc_client_call(IntPtr client, byte[] method, byte[] pars, UIntPtr parsLen, IntPtr opts, out Buffer result);
        [DllImport(Lib)] public static extern void stintmate_ipc_unsubscribe(IntPtr subscription);

        /// <summary>A NUL-terminated UTF-8 copy of a string, for the const char* parameters.</summary>
        public static byte[] Utf8(string s)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(s);
            byte[] z = new byte[bytes.Length + 1];
            Array.Copy(bytes, z, bytes.Length);
            return z;
        }

        public static string LastError()
        {
            byte[] buf = new byte[512];
            stintmate_ipc_last_error_message(buf, (UIntPtr)buf.Length);
            return Encoding.UTF8.GetString(buf).TrimEnd('\0');
        }
    }
}
