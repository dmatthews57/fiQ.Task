using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.BouncyCastle.Bcpg;
using Org.BouncyCastle.Bcpg.OpenPgp;
using Org.BouncyCastle.Security;

namespace fiQ.Task.Utilities
{
	public static class PGPUtilities
	{
		#region Public methods
		/// <summary>
		/// PGP-encrypt data from source to destination stream in raw/binary format
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key will be used)</param>
		/// <param name="clearinput">Input stream containing source data to be encrypted</param>
		/// <param name="encryptedoutput">Output stream to receive encrypted data</param>
		/// <remarks>
		/// Public key will be read synchronously from source (if this is a concern, caller should pull into local stream asynchronously first)
		/// </remarks>
		public static async System.Threading.Tasks.Task EncryptRaw(Stream publickeysource, string publickeyuserid, Stream clearinput, Stream encryptedoutput)
		{
			// Create encrypted data generator, using public key extracted from provided source:
			var pgpEncDataGen = new PgpEncryptedDataGenerator(SymmetricKeyAlgorithmTag.Aes256, true, new SecureRandom());
			pgpEncDataGen.AddMethod(ExtractPublicKey(publickeysource, publickeyuserid));

			// Wrap destination stream in encrypted data generator stream:
			using (var encrypt = pgpEncDataGen.Open(encryptedoutput, new byte[1024]))
			{
				// Wrap encrypted data generator stream in compressed data generator stream:
				var comData = new PgpCompressedDataGenerator(CompressionAlgorithmTag.Zip);
				using (var compress = comData.Open(encrypt))
				{
					// Wrap compressed data generator stream in literal data generator stream:
					var lData = new PgpLiteralDataGenerator();
					using (var literal = lData.Open(compress, PgpLiteralData.Binary, string.Empty, DateTime.UtcNow, new byte[1024]))
					{
						// Stream source data into literal generator (whose output will feed into compression stream,
						// whose output will feed into encryption stream, whose output will feed into destination):
						await clearinput.CopyToAsync(literal);
					}
				}
			}
		}
		/// <summary>
		/// PGP-encrypt data from source to destination stream in ASCII-armored format
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key will be used)</param>
		/// <param name="clearinput">Input stream containing source data to be encrypted</param>
		/// <param name="encryptedoutput">Output stream to receive encrypted data</param>
		/// <remarks>
		/// Public key will be read synchronously from source (if this is a concern, caller should pull into local stream asynchronously first)
		/// </remarks>
		public static async System.Threading.Tasks.Task Encrypt(Stream publickeysource, string publickeyuserid, Stream clearinput, Stream encryptedoutput)
		{
			// Call raw encryption function with armored output stream wrapping destination:
			using (var armor = new ArmoredOutputStream(encryptedoutput))
			{
				await EncryptRaw(publickeysource, publickeyuserid, clearinput, armor);
			}
		}

		/// <summary>
		/// PGP-decrypt data from source to destination stream
		/// </summary>
		/// <param name="privatekeysource">Input stream containing private keyring bundle</param>
		/// <param name="privatekeypassphrase">Passphrase to access private key (if required)</param>
		/// <param name="encryptedinput">Input stream containing source data to be decrypted</param>
		/// <param name="clearoutput">Output stream to receive decrypted data</param>
		/// <remarks>
		/// - Private key will be read synchronously from source (if this is a concern, caller should pull into local stream asynchronously first)
		/// - Destination data stream will be written asynchronously, but source data stream will be read synchronously (due to limitation in PGP
		/// library; again if a concern, caller should pull data into a local stream asynchronously before calling this function)
		/// </remarks>
		public static async System.Threading.Tasks.Task Decrypt(Stream privatekeysource, string privatekeypassphrase, Stream encryptedinput, Stream clearoutput)
		{
			// Create object factory using DecoderStream to read PGP data from source, use factory to extract encrypted data:
			using (var decoder = PgpUtilities.GetDecoderStream(encryptedinput))
			{
				var encrypteddata = new PgpObjectFactory(decoder).GetEncryptedData() ?? throw new ArgumentException("No PGP-encrypted data found");

				// Extract private key from key source (using key ID required by encrypted data object), use to open clear stream:
				using (var clearsource = encrypteddata.GetDataStream(ExtractPrivateKey(privatekeysource, privatekeypassphrase, encrypteddata.KeyId)))
				{
					// Create new object factory from clear stream and use to write cleartext data to destination:
					await new PgpObjectFactory(clearsource).WriteClearData(clearoutput);
				}
			}
		}

		/// <summary>
		/// Create stream from a PGP-encrypted input stream, from which PGP-decrypted data can be read
		/// </summary>
		/// <param name="privatekeysource">Input stream containing private keyring bundle</param>
		/// <param name="privatekeypassphrase">Passphrase to access private key (if required)</param>
		/// <param name="encryptedinput">Input stream containing source data to be decrypted</param>
		/// <returns>
		/// A DecryptionStream object (caller is responsible for disposing)
		/// </returns>
		/// <remarks>
		/// - Private key will be read synchronously from source (if this is a concern, caller should pull into local stream asynchronously first)
		/// - Source data stream will be read synchronously (again if a concern, caller should pull data into a local stream asynchronously before calling this function)
		/// </remarks>
		public static DecryptionStream GetDecryptionStream(Stream privatekeysource, string privatekeypassphrase, Stream encryptedinput)
		{
			var decryptionstream = new DecryptionStream();
			try
			{
				// Open decoder stream and push on to return value stack:
				decryptionstream.PushStream(PgpUtilities.GetDecoderStream(encryptedinput));

				// Create object factory (using stream at top of stack) to extract encrypted data:
				var factory = new PgpObjectFactory(decryptionstream.GetStream());
				var encrypteddata = factory.GetEncryptedData() ?? throw new ArgumentException("No PGP-encrypted data found");

				// Extract private key from key source (using key ID required by encrypted data object), use to open
				// clear stream and push on to return value stack:
				decryptionstream.PushStream(encrypteddata.GetDataStream(ExtractPrivateKey(privatekeysource, privatekeypassphrase, encrypteddata.KeyId)));

				// Create new factory from clear stream and extract first PGP object:
				factory = new PgpObjectFactory(decryptionstream.GetStream());
				var message = factory.NextPgpObject();
				while (message != null)
				{
					// If object is literal data, push de-literalization stream on to return value stack and return:
					if (message is PgpLiteralData cleardata)
					{
						decryptionstream.PushStream(cleardata.GetInputStream());
						return decryptionstream;
					}
					// Otherwise if this is compressed data, we need to push decompression stream on to return value
					// stack, then create a new object factory to extract data from decompressed source:
					else if (message is PgpCompressedData compresseddata)
					{
						decryptionstream.PushStream(compresseddata.GetDataStream());
						factory = new PgpObjectFactory(decryptionstream.GetStream());
					}

					// Extract next message object from factory and continue:
					message = factory.NextPgpObject();
				}

				// If this point is reached, no literal packet was ever found
				throw new ArgumentException("Invalid PGP data (no payload found)");
			}
			catch
			{
				decryptionstream.Dispose();
				throw;
			}
		}
		#endregion

		#region Private methods
		private static PgpPrivateKey ExtractPrivateKey(Stream keysource, string passphrase, long keyID)
		{
			try
			{
				// Use DecoderStream to read secret keyring bundle from stream, retrieve the secret
				// key based on requested keyID, and extract using provided passphrase (if any):
				using var decoderstream = PgpUtilities.GetDecoderStream(keysource);
				return new PgpSecretKeyRingBundle(decoderstream)
					.GetSecretKey(keyID)
					?.ExtractPrivateKey((passphrase ?? string.Empty).ToCharArray()) ?? throw new ArgumentException("Invalid key ring or private key not found");
			}
			catch (PgpException p)
			{
				if ((uint)p.HResult == 0x80131500) // Catch specific case of invalid passphrase
				{
					throw new ArgumentException("Invalid passphrase, key could not be extracted");
				}
				else // In any other case, just re-throw exception:
				{
					throw;
				}
			}
		}
		private static PgpPublicKey ExtractPublicKey(Stream keysource, string userid)
		{
			// Use DecoderStream to read public keyring bundle from stream:
			using var decoderstream = PgpUtilities.GetDecoderStream(keysource);
			return new PgpPublicKeyRingBundle(decoderstream)
				// Extract public keyrings:
				.GetKeyRings()
				.Cast<PgpPublicKeyRing>()
				// Select public key out of rings, where key is usable for encryption and has a matching UserID entry (if requested):
				.Select(keyring =>
					keyring.GetPublicKeys()
					.Cast<PgpPublicKey>()
					.FirstOrDefault(key => key.IsEncryptionKey && (
						string.IsNullOrEmpty(userid)
						|| key.GetUserIds().Cast<string>().Any(userid => userid.Equals(userid, StringComparison.OrdinalIgnoreCase)))
					)
				)
				// Choose first non-null public key emitted from selection above:
				?.FirstOrDefault(key => key != null) ?? throw new ArgumentException("Invalid key ring or public key not found");
		}
		#endregion

		#region Classes
		/// <summary>
		/// Wrapper class to hold series of streams necessary for PGP decryption, and handle disposal
		/// </summary>
		public class DecryptionStream : IDisposable
		{
			#region Fields
			private Stack<Stream> streams = new Stack<Stream>();
			private bool disposed = false;
			#endregion

			#region IDisposable implementation
			public void Dispose()
			{
				Dispose(true);
				GC.SuppressFinalize(this);
			}
			protected virtual void Dispose(bool disposing)
			{
				if (disposed == false && disposing)
				{
					while (streams.Count > 0)
					{
						var stream = streams.Pop();
						stream.Dispose();
					}
				}
				disposed = true;
			}
			#endregion

			#region Methods
			/// <summary>
			/// Retrieve stream at top of stack
			/// </summary>
			public Stream GetStream()
			{
				return streams.Count > 0 ? streams.Peek() : null;
			}
			/// <summary>
			/// Add stream to top of stack
			/// </summary>
			internal void PushStream(Stream stream)
			{
				streams.Push(stream);
			}
			#endregion
		}
		#endregion
	}

	public static class PgpObjectFactoryExtensions
	{
		/// <summary>
		/// Extract PgpPublicKeyEncryptedData from PgpObjectFactory created from source encrypted stream
		/// </summary>
		public static PgpPublicKeyEncryptedData GetEncryptedData(this PgpObjectFactory factory)
		{
			try
			{
				// Extract first PGP object from factory:
				var obj = factory.NextPgpObject();
				while (obj != null)
				{
					// If object is an encrypted data list, extract first encrypted data object:
					if (obj is PgpEncryptedDataList list)
					{
						return list
							.GetEncryptedDataObjects()
							.Cast<PgpPublicKeyEncryptedData>()
							.FirstOrDefault();
					}
					// Otherwise, move on to next object:
					obj = factory.NextPgpObject();
				}
				// If this point is reached, encrypted data list was never encountered
				return null;
			}
			catch (Exception ex)
			{
				// Note that NextPgpObject will throw IOException if unknown object type encountered
				// prior to capturing a PgpEncryptedDataList object (likely what happened here)
				throw new ArgumentException("Failed to extract PGP-encrypted data", ex);
			}
		}

		/// <summary>
		/// Retrieve cleartext literal packet data read from PgpObjectFactory (created from PgpEncryptedData
		/// combined with PgpPrivateKey) and write payload asynchronously into destination stream
		/// </summary>
		public static async System.Threading.Tasks.Task WriteClearData(this PgpObjectFactory factory, Stream cleardest)
		{
			// Extract first PGP object from factory:
			var message = factory.NextPgpObject();
			while (message != null)
			{
				// If object is literal data, de-literalize to destination and exit:
				if (message is PgpLiteralData cleardata)
				{
					using (var clearstream = cleardata.GetInputStream())
					{
						await clearstream.CopyToAsync(cleardest);
						return;
					}
				}
				// Otherwise if this is compressed data, we need to create a new object factory to
				// decompress data from compressed source and call this function recursively:
				else if (message is PgpCompressedData compresseddata)
				{
					using (var compresseddatastream = compresseddata.GetDataStream())
					{
						await new PgpObjectFactory(compresseddatastream).WriteClearData(cleardest);
						return;
					}
				}
				// Otherwise continue on to next message object
				message = factory.NextPgpObject();
			}

			// If this point is reached, no literal packet was ever found
			throw new ArgumentException("Invalid PGP data (no payload found)");
		}
	}
}
