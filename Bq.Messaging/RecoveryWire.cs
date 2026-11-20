using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using MessagePack;
using MessagePack.Resolvers;
using RabbitMQ.Client;

namespace Bq.Messaging
{
    public sealed class RecoveryEnvelope
    {
        public string MessageId { get; set; }
        public string Type { get; set; }
        public string ContentType { get; set; }
        public string ContentEncoding { get; set; }
        public string CorrelationId { get; set; }
        public string ReplyTo { get; set; }
        public string AppId { get; set; }
        public string UserId { get; set; }
        public string Expiration { get; set; }
        public string ClusterId { get; set; }
        public byte Priority { get; set; }
        public byte DeliveryMode { get; set; }
        public long Timestamp { get; set; }
        public byte[] Headers { get; set; }
        public byte[] Body { get; set; }
    }
    internal static class RecoveryWire
    {
        const string CycleHeader = "x-bq-retry-cycle";
        const string OriginalIdHeader = "x-bq-original-message-id";
        static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance).WithSecurity(MessagePackSecurity.UntrustedData);
        public static RecoveryEnvelope Capture(IBasicProperties p, ReadOnlyMemory<byte> body)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8))
            {
                WriteValue(writer, p.Headers ?? new Dictionary<string, object>(), 0);
                return new RecoveryEnvelope { MessageId = p.MessageId, Type = p.Type, ContentType = p.ContentType, ContentEncoding = p.ContentEncoding,
                    CorrelationId = p.CorrelationId, ReplyTo = p.ReplyTo, AppId = p.AppId, UserId = p.UserId, Expiration = p.Expiration,
                    ClusterId = p.ClusterId, Priority = p.Priority, DeliveryMode = p.DeliveryMode, Timestamp = p.Timestamp.UnixTime,
                    Headers = stream.ToArray(), Body = body.ToArray() };
            }
        }
        public static byte[] Encode(RecoveryEnvelope envelope) => MessagePackSerializer.Serialize(envelope, Options);
        public static RecoveryEnvelope Decode(byte[] bytes) => MessagePackSerializer.Deserialize<RecoveryEnvelope>(bytes, Options);
        public static IDictionary<string, object> Headers(RecoveryEnvelope envelope)
        {
            using (var stream = new MemoryStream(envelope.Headers, false))
            using (var reader = new BinaryReader(stream, Encoding.UTF8)) return (IDictionary<string, object>)ReadValue(reader, 0);
        }
        public static void SetHeaders(RecoveryEnvelope envelope, IDictionary<string, object> headers)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8)) { WriteValue(writer, headers, 0); envelope.Headers = stream.ToArray(); }
        }
        public static byte[] EncodeHeaders(IDictionary<string, object> headers)
        {
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream, Encoding.UTF8))
            {
                WriteValue(writer, headers ?? new Dictionary<string, object>(), 0);
                return stream.ToArray();
            }
        }
        public static IDictionary<string, object> DecodeHeaders(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0) return new Dictionary<string, object>();
            using (var stream = new MemoryStream(bytes, false))
            using (var reader = new BinaryReader(stream, Encoding.UTF8))
            {
                var value = ReadValue(reader, 0) as IDictionary<string, object>;
                if (value == null || stream.Position != stream.Length) throw new InvalidDataException("Invalid persisted message headers.");
                return value;
            }
        }
        public static int Cycle(IBasicProperties properties)
        {
            if (properties.Headers == null || !properties.Headers.TryGetValue(CycleHeader, out var value)) return 0;
            if (!(value is int cycle) || cycle < 0) throw new InvalidOperationException("Invalid retry cycle header.");
            return cycle;
        }
        public static void SetCycle(RecoveryEnvelope envelope, int cycle)
        {
            var headers = Headers(envelope); headers[CycleHeader] = cycle; SetHeaders(envelope, headers);
        }
        public static void SetFailure(RecoveryEnvelope envelope, Exception failure, int cycle, string endpoint)
        {
            var headers = Headers(envelope);
            headers["x-bq-failed-endpoint"] = endpoint;
            headers["x-bq-failure-type"] = failure.GetType().FullName;
            headers["x-bq-failure-message"] = failure.Message ?? "";
            headers["x-bq-failure-detail"] = failure.ToString();
            headers["x-bq-failure-cycle"] = cycle;
            SetHeaders(envelope, headers);
        }
        public static void SetOriginalId(RecoveryEnvelope envelope, string originalId)
        {
            var headers = Headers(envelope); headers[OriginalIdHeader] = originalId; headers[CycleHeader] = 0; SetHeaders(envelope, headers);
        }
        public static void CustomizeHeaders(RecoveryEnvelope envelope, Action<Dictionary<string, string>> customize)
        {
            var headers = Headers(envelope);
            var original = MessageHeaders.AsText(headers);
            var changed = original.ToDictionary(header => header.Key, header => header.Value, StringComparer.Ordinal);
            customize(changed);
            ApplyTextHeaderEdits(headers, original, changed);
            SetHeaders(envelope, headers);
        }

        internal static void ApplyTextHeaderEdits(IDictionary<string, object> headers,
            IReadOnlyDictionary<string, string> original, Dictionary<string, string> changed)
        {
            foreach (var key in original.Keys)
                if (!changed.ContainsKey(key)) headers.Remove(key);
            foreach (var header in changed)
                if (!original.TryGetValue(header.Key, out var value) || value != header.Value)
                    headers[header.Key] = header.Value;
        }
        public static void Apply(RecoveryEnvelope e, IBasicProperties p)
        {
            p.MessageId = e.MessageId; p.Type = e.Type; p.ContentType = e.ContentType; p.ContentEncoding = e.ContentEncoding;
            p.CorrelationId = e.CorrelationId; p.ReplyTo = e.ReplyTo; p.AppId = e.AppId; p.UserId = e.UserId; p.Expiration = e.Expiration;
            p.ClusterId = e.ClusterId; p.Priority = e.Priority; p.DeliveryMode = e.DeliveryMode; p.Timestamp = new AmqpTimestamp(e.Timestamp); p.Headers = Headers(e);
        }
        static void WriteValue(BinaryWriter w, object value, int depth)
        {
            if (depth > 32) throw new InvalidDataException("AMQP header nesting is too deep.");
            if (value == null) { w.Write((byte)0); return; }
            if (value is byte[] bytes) { w.Write((byte)1); w.Write(bytes.Length); w.Write(bytes); return; }
            if (value is string text) { w.Write((byte)2); w.Write(text); return; }
            if (value is bool boolean) { w.Write((byte)3); w.Write(boolean); return; }
            if (value is int i32) { w.Write((byte)4); w.Write(i32); return; }
            if (value is long i64) { w.Write((byte)5); w.Write(i64); return; }
            if (value is short i16) { w.Write((byte)6); w.Write(i16); return; }
            if (value is byte u8) { w.Write((byte)7); w.Write(u8); return; }
            if (value is sbyte i8) { w.Write((byte)8); w.Write(i8); return; }
            if (value is uint u32) { w.Write((byte)9); w.Write(u32); return; }
            if (value is ulong u64) { w.Write((byte)10); w.Write(u64); return; }
            if (value is ushort u16) { w.Write((byte)11); w.Write(u16); return; }
            if (value is float f32) { w.Write((byte)12); w.Write(f32); return; }
            if (value is double f64) { w.Write((byte)13); w.Write(f64); return; }
            if (value is decimal number) { w.Write((byte)14); w.Write(number); return; }
            if (value is AmqpTimestamp timestamp) { w.Write((byte)15); w.Write(timestamp.UnixTime); return; }
            if (value is IDictionary<string, object> table)
            {
                w.Write((byte)16); w.Write(table.Count);
                foreach (var item in table) { w.Write(item.Key); WriteValue(w, item.Value, depth + 1); }
                return;
            }
            if (value is IList list)
            {
                w.Write((byte)17); w.Write(list.Count);
                foreach (var item in list) WriteValue(w, item, depth + 1);
                return;
            }
            throw new InvalidDataException("Unsupported AMQP header type: " + value.GetType().FullName);
        }
        static int Count(BinaryReader r) { var count = r.ReadInt32(); if (count < 0 || count > 1000000) throw new InvalidDataException("Invalid AMQP header length."); return count; }
        static object ReadValue(BinaryReader r, int depth)
        {
            if (depth > 32) throw new InvalidDataException("AMQP header nesting is too deep.");
            switch (r.ReadByte())
            {
                case 0: return null;
                case 1: { var count = Count(r); var bytes = r.ReadBytes(count); if (bytes.Length != count) throw new EndOfStreamException(); return bytes; }
                case 2: return r.ReadString(); case 3: return r.ReadBoolean(); case 4: return r.ReadInt32(); case 5: return r.ReadInt64();
                case 6: return r.ReadInt16(); case 7: return r.ReadByte(); case 8: return r.ReadSByte(); case 9: return r.ReadUInt32();
                case 10: return r.ReadUInt64(); case 11: return r.ReadUInt16(); case 12: return r.ReadSingle(); case 13: return r.ReadDouble();
                case 14: return r.ReadDecimal(); case 15: return new AmqpTimestamp(r.ReadInt64());
                case 16: { var table = new Dictionary<string, object>(); var n = Count(r); for (var j = 0; j < n; j++) table.Add(r.ReadString(), ReadValue(r, depth + 1)); return table; }
                case 17: { var list = new List<object>(); var n = Count(r); for (var j = 0; j < n; j++) list.Add(ReadValue(r, depth + 1)); return list; }
                default: throw new InvalidDataException("Invalid AMQP header value.");
            }
        }
    }
}
