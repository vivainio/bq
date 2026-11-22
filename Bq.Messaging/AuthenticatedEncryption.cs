using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Bq.Messaging
{
    public static class ConfigureAuthenticatedEncryptionService
    {
        /// <summary>
        /// Encrypts <see cref="WireEncryptedString"/> properties with AES-CBC and authenticates them with HMAC-SHA256 (encrypt then MAC).
        /// Unlike the Rijndael service, a wrong key or a changed value is always detected. Keys are 32 bytes. This format is Bq's own
        /// and is not readable by NServiceBus.
        /// </summary>
        public static void AuthenticatedEncryptionService(this EndpointConfiguration configuration, string encryptionKeyIdentifier, byte[] encryptionKey,
            IList<byte[]> expiredKeys = null)
        {
            if (encryptionKey == null) throw new ArgumentNullException(nameof(encryptionKey));
            configuration.AuthenticatedEncryptionService(encryptionKeyIdentifier,
                new Dictionary<string, byte[]> { [encryptionKeyIdentifier ?? throw new ArgumentNullException(nameof(encryptionKeyIdentifier))] = encryptionKey }, expiredKeys);
        }

        public static void AuthenticatedEncryptionService(this EndpointConfiguration configuration, string encryptionKeyIdentifier,
            IDictionary<string, byte[]> keys, IList<byte[]> expiredKeys = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var service = new AuthenticatedEncryptionServiceImplementation(encryptionKeyIdentifier, keys, expiredKeys ?? new List<byte[]>());
            configuration.RegisterEncryptionService(() => service);
        }
    }

    sealed class AuthenticatedEncryptionServiceImplementation : IEncryptionService
    {
        public const string KeyIdentifierHeader = "Bq.EncryptionKeyIdentifier";
        const int TagSize = 32;
        static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false, true);

        readonly string keyIdentifier;
        readonly IDictionary<string, byte[]> keys;
        readonly IList<byte[]> expiredKeys;

        public AuthenticatedEncryptionServiceImplementation(string keyIdentifier, IDictionary<string, byte[]> keys, IList<byte[]> expiredKeys)
        {
            this.keyIdentifier = keyIdentifier;
            this.keys = keys ?? throw new ArgumentNullException(nameof(keys));
            this.expiredKeys = expiredKeys;
            if (!keys.ContainsKey(keyIdentifier ?? "")) throw new ArgumentException("The key identifier must name one of the keys.", nameof(keyIdentifier));
            foreach (var key in keys.Values.Concat(expiredKeys))
                if (key == null || key.Length != 32) throw new ArgumentException("Authenticated encryption keys must be 32 bytes.");
        }

        // One master key gives two independent keys, so the same bytes never encrypt and authenticate.
        static byte[] Derive(byte[] master, string purpose)
        {
            using (var hmac = new HMACSHA256(master)) return hmac.ComputeHash(Encoding.ASCII.GetBytes(purpose));
        }

        static byte[] Tag(byte[] master, string id, byte[] iv, byte[] cipher)
        {
            using (var hmac = new HMACSHA256(Derive(master, "bq-mac")))
            {
                var name = Encoding.UTF8.GetBytes(id ?? "");
                hmac.TransformBlock(BitConverter.GetBytes(name.Length), 0, 4, null, 0);
                hmac.TransformBlock(name, 0, name.Length, null, 0);
                hmac.TransformBlock(iv, 0, iv.Length, null, 0);
                hmac.TransformFinalBlock(cipher, 0, cipher.Length);
                return hmac.Hash;
            }
        }

        static bool SameBytes(byte[] a, byte[] b)
        {
            var difference = a.Length ^ b.Length;
            for (var i = 0; i < a.Length && i < b.Length; i++) difference |= a[i] ^ b[i];
            return difference == 0;
        }

        public EncryptedValue Encrypt(string value, IOutgoingLogicalMessageContext context)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            context.Headers[KeyIdentifierHeader] = keyIdentifier;
            var master = keys[keyIdentifier];
            using (var aes = Aes.Create())
            {
                aes.Key = Derive(master, "bq-enc");
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                aes.GenerateIV();
                byte[] cipher;
                using (var encryptor = aes.CreateEncryptor())
                {
                    var plain = Encoding.UTF8.GetBytes(value);
                    cipher = encryptor.TransformFinalBlock(plain, 0, plain.Length);
                }
                var sealedValue = cipher.Concat(Tag(master, keyIdentifier, aes.IV, cipher)).ToArray();
                return new EncryptedValue { EncryptedBase64Value = Convert.ToBase64String(sealedValue), Base64Iv = Convert.ToBase64String(aes.IV) };
            }
        }

        public string Decrypt(EncryptedValue encryptedValue, IIncomingLogicalMessageContext context)
        {
            if (encryptedValue == null) throw new ArgumentNullException(nameof(encryptedValue));
            var iv = Convert.FromBase64String(encryptedValue.Base64Iv);
            var sealedValue = Convert.FromBase64String(encryptedValue.EncryptedBase64Value);
            if (sealedValue.Length <= TagSize) throw new InvalidOperationException("The encrypted value is too short.");
            var cipher = sealedValue.Take(sealedValue.Length - TagSize).ToArray();
            var tag = sealedValue.Skip(cipher.Length).ToArray();

            // The sender names its key; keys retired since may still have encrypted messages in flight.
            var candidates = new List<KeyValuePair<string, byte[]>>();
            if (context.Headers.TryGetValue(KeyIdentifierHeader, out var id) && keys.TryGetValue(id, out var named))
                candidates.Add(new KeyValuePair<string, byte[]>(id, named));
            candidates.AddRange(keys.Where(pair => pair.Key != id));
            // An expired key has no name here; the sender's header still names it, so authenticate under that name.
            candidates.AddRange(expiredKeys.Select(key => new KeyValuePair<string, byte[]>(id, key)));

            foreach (var candidate in candidates)
            {
                if (!SameBytes(Tag(candidate.Value, candidate.Key, iv, cipher), tag)) continue;
                using (var aes = Aes.Create())
                {
                    aes.Key = Derive(candidate.Value, "bq-enc");
                    aes.IV = iv;
                    aes.Mode = CipherMode.CBC;
                    aes.Padding = PaddingMode.PKCS7;
                    using (var decryptor = aes.CreateDecryptor())
                        return StrictUtf8.GetString(decryptor.TransformFinalBlock(cipher, 0, cipher.Length));
                }
            }
            throw new InvalidOperationException("The value could not be authenticated with the current or expired keys. It may have been changed.");
        }
    }
}
