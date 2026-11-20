using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IMutateOutgoingMessages
    {
        Task MutateOutgoing(MutateOutgoingMessageContext context);
    }

    public sealed class MutateOutgoingMessageContext
    {
        readonly object incomingMessage;
        readonly IReadOnlyDictionary<string, string> incomingHeaders;

        public MutateOutgoingMessageContext(object outgoingMessage,
            Dictionary<string, string> outgoingHeaders, object incomingMessage,
            IReadOnlyDictionary<string, string> incomingHeaders)
        {
            OutgoingMessage = outgoingMessage;
            OutgoingHeaders = outgoingHeaders;
            this.incomingMessage = incomingMessage;
            this.incomingHeaders = incomingHeaders;
        }

        public object OutgoingMessage { get; set; }
        public Dictionary<string, string> OutgoingHeaders { get; }

        public bool TryGetIncomingMessage(out object message)
        {
            message = incomingMessage;
            return message != null;
        }

        public bool TryGetIncomingHeaders(out IReadOnlyDictionary<string, string> headers)
        {
            headers = incomingHeaders;
            return headers != null;
        }
    }

    public interface IMutateOutgoingTransportMessages
    {
        Task MutateOutgoing(MutateOutgoingTransportMessageContext context);
    }

    public sealed class MutateOutgoingTransportMessageContext
    {
        readonly object incomingMessage;
        readonly IReadOnlyDictionary<string, string> incomingHeaders;

        public MutateOutgoingTransportMessageContext(byte[] outgoingBody, object outgoingMessage,
            Dictionary<string, string> outgoingHeaders, object incomingMessage,
            IReadOnlyDictionary<string, string> incomingHeaders)
        {
            OutgoingBody = outgoingBody;
            OutgoingMessage = outgoingMessage;
            OutgoingHeaders = outgoingHeaders;
            this.incomingMessage = incomingMessage;
            this.incomingHeaders = incomingHeaders;
        }

        public byte[] OutgoingBody { get; set; }
        public object OutgoingMessage { get; }
        public Dictionary<string, string> OutgoingHeaders { get; }

        public bool TryGetIncomingMessage(out object message)
        {
            message = incomingMessage;
            return message != null;
        }

        public bool TryGetIncomingHeaders(out IReadOnlyDictionary<string, string> headers)
        {
            headers = incomingHeaders;
            return headers != null;
        }
    }

    public interface IMutateIncomingTransportMessages
    {
        Task MutateIncoming(MutateIncomingTransportMessageContext context);
    }

    public sealed class MutateIncomingTransportMessageContext
    {
        public MutateIncomingTransportMessageContext(byte[] body, Dictionary<string, string> headers)
        {
            Body = body ?? throw new ArgumentNullException(nameof(body));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        public byte[] Body { get; set; }
        public Dictionary<string, string> Headers { get; }
    }

    public interface IMutateIncomingMessages
    {
        Task MutateIncoming(MutateIncomingMessageContext context);
    }

    public sealed class MutateIncomingMessageContext
    {
        public MutateIncomingMessageContext(object message, Dictionary<string, string> headers)
        {
            Message = message ?? throw new ArgumentNullException(nameof(message));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        public object Message { get; set; }
        public Dictionary<string, string> Headers { get; }
    }

    public sealed class TransportMessage
    {
        internal TransportMessage(string typeName, byte[] body, IDictionary<string, object> headers,
            IMessageHandlerContext incomingMessage = null)
        {
            TypeName = typeName;
            Body = body;
            Headers = headers;
            IncomingMessage = incomingMessage;
        }

        public string TypeName { get; set; }
        public byte[] Body { get; set; }
        public IDictionary<string, object> Headers { get; }
        public IMessageHandlerContext IncomingMessage { get; }
    }
}
