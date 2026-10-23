// A minimal Stintmate IPC client in C#: calls "t.echo" and checks the answer.
//   Echo.exe <pipe path> <key file>
// Written in a C# 5 subset so it also builds with the csc.exe that ships with Windows.
using System;
using System.Runtime.InteropServices;
using Stintmate.Ipc;

public static class Echo
{
    public static int Main(string[] args)
    {
        if (args.Length != 2)
        {
            Console.Error.WriteLine("usage: Echo <pipe path> <key file>");
            return 2;
        }
        Console.WriteLine("stintmate-ipc " + Marshal.PtrToStringAnsi(Native.stintmate_ipc_version()));

        PeerInfo info = new PeerInfo();
        info.App = Marshal.StringToHGlobalAnsi("csharp-example");
        info.Name = Marshal.StringToHGlobalAnsi("C# example");
        info.Version = Marshal.StringToHGlobalAnsi("1.0");

        IntPtr client;
        if (Native.stintmate_ipc_client_new(Native.Utf8(args[0]), ref info, Native.Utf8(args[1]), out client) != Native.Ok)
        {
            Console.Error.WriteLine("client_new failed: " + Native.LastError());
            return 1;
        }
        Native.stintmate_ipc_client_start(client);
        if (Native.stintmate_ipc_client_wait_connected(client, 5000) != Native.Ok)
        {
            Console.Error.WriteLine("wait_connected failed: " + Native.LastError());
            Native.stintmate_ipc_client_free(client);
            return 1;
        }

        // MessagePack fixstr "hello".
        byte[] pars = new byte[] { 0xa5, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        Stintmate.Ipc.Buffer result;
        int status = Native.stintmate_ipc_call(client, Native.Utf8("t.echo"), 1, pars, (UIntPtr)pars.Length, 2000, out result);
        bool ok = false;
        if (status == Native.Ok && (int)result.Len == pars.Length)
        {
            byte[] got = new byte[pars.Length];
            Marshal.Copy(result.Ptr, got, 0, got.Length);
            ok = BitConverter.ToString(got) == BitConverter.ToString(pars);
        }
        else if (status != Native.Ok)
        {
            Console.Error.WriteLine("call failed: " + Native.LastError());
        }
        Native.stintmate_ipc_free_buffer(result);
        Native.stintmate_ipc_client_free(client);
        if (!ok)
        {
            return 1;
        }
        Console.WriteLine("echo ok");
        return 0;
    }
}
