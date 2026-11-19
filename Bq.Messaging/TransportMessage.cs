using System.Collections.Generic;

namespace Bq.Messaging
{
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
