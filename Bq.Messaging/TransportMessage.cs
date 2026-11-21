using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
    public interface IMutateOutgoingMessages
    {
        Task MutateOutgoing(MutateOutgoingMessageContext context);
    }

    public class MutateOutgoingMessageContext
    {
        readonly object incomingMessage;
        readonly IReadOnlyDictionary<string, string> incomingHeaders;

        public MutateOutgoingMessageContext(object outgoingMessage,
            Dictionary<string, string> outgoingHeaders, object incomingMessage,
            IReadOnlyDictionary<string, string> incomingHeaders)
        {
            currentOutgoingMessage = outgoingMessage ?? throw new ArgumentNullException(nameof(outgoingMessage));
            OutgoingHeaders = outgoingHeaders ?? throw new ArgumentNullException(nameof(outgoingHeaders));
            this.incomingMessage = incomingMessage;
            this.incomingHeaders = incomingHeaders;
        }

        object currentOutgoingMessage;
        public object OutgoingMessage
        {
            get => currentOutgoingMessage;
            set => currentOutgoingMessage = value ?? throw new ArgumentNullException(nameof(value));
        }
        public Dictionary<string, string> OutgoingHeaders { get; }

        public bool TryGetIncomingMessage(out object incomingMessage)
        {
            incomingMessage = this.incomingMessage;
            return incomingMessage != null;
        }

        public bool TryGetIncomingHeaders(out IReadOnlyDictionary<string, string> incomingHeaders)
        {
            incomingHeaders = this.incomingHeaders;
            return incomingHeaders != null;
        }
    }

    public interface IMutateOutgoingTransportMessages
    {
        Task MutateOutgoing(MutateOutgoingTransportMessageContext context);
    }

    public class MutateOutgoingTransportMessageContext
    {
        readonly object incomingMessage;
        readonly IReadOnlyDictionary<string, string> incomingHeaders;

        public MutateOutgoingTransportMessageContext(byte[] outgoingBody, object outgoingMessage,
            Dictionary<string, string> outgoingHeaders, object incomingMessage,
            IReadOnlyDictionary<string, string> incomingHeaders)
        {
            currentOutgoingBody = outgoingBody ?? throw new ArgumentNullException(nameof(outgoingBody));
            OutgoingMessage = outgoingMessage ?? throw new ArgumentNullException(nameof(outgoingMessage));
            OutgoingHeaders = outgoingHeaders ?? throw new ArgumentNullException(nameof(outgoingHeaders));
            this.incomingMessage = incomingMessage;
            this.incomingHeaders = incomingHeaders;
        }

        byte[] currentOutgoingBody;
        public byte[] OutgoingBody
        {
            get => currentOutgoingBody;
            set => currentOutgoingBody = value ?? throw new ArgumentNullException(nameof(value));
        }
        public object OutgoingMessage { get; }
        public Dictionary<string, string> OutgoingHeaders { get; }

        public bool TryGetIncomingMessage(out object incomingMessage)
        {
            incomingMessage = this.incomingMessage;
            return incomingMessage != null;
        }

        public bool TryGetIncomingHeaders(out IReadOnlyDictionary<string, string> incomingHeaders)
        {
            incomingHeaders = this.incomingHeaders;
            return incomingHeaders != null;
        }
    }

    public interface IMutateIncomingTransportMessages
    {
        Task MutateIncoming(MutateIncomingTransportMessageContext context);
    }

    public class MutateIncomingTransportMessageContext
    {
        public MutateIncomingTransportMessageContext(byte[] body, Dictionary<string, string> headers)
        {
            currentBody = body ?? throw new ArgumentNullException(nameof(body));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        byte[] currentBody;
        public byte[] Body
        {
            get => currentBody;
            set => currentBody = value ?? throw new ArgumentNullException(nameof(value));
        }
        public Dictionary<string, string> Headers { get; }
    }

    public interface IMutateIncomingMessages
    {
        Task MutateIncoming(MutateIncomingMessageContext context);
    }

    public class MutateIncomingMessageContext
    {
        public MutateIncomingMessageContext(object message, Dictionary<string, string> headers)
        {
            currentMessage = message ?? throw new ArgumentNullException(nameof(message));
            Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        }

        object currentMessage;
        public object Message
        {
            get => currentMessage;
            set => currentMessage = value ?? throw new ArgumentNullException(nameof(value));
        }
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
