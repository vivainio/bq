using System;

namespace Bq.Messaging
{
    public interface IMessageBodyEncryptionService
    {
        byte[] Encrypt(byte[] body);
        byte[] Decrypt(byte[] body);
    }

    public static class MessageBodyEncryptionExtensions
    {
        public static void EnableMessageEncryption(this EndpointConfiguration configuration, IMessageBodyEncryptionService encryptionService)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            configuration.BodyEncryption = encryptionService ?? throw new ArgumentNullException(nameof(encryptionService));
        }
    }
}
