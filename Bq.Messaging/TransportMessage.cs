using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bq.Messaging
{
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
