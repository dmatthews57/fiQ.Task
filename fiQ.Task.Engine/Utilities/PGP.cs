using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Org.BouncyCastle.Bcpg;
using Org.BouncyCastle.Bcpg.OpenPgp;
using Org.BouncyCastle.Security;

namespace fiQ.TaskUtilities
{
	public static class Pgp
	{
		#region Public methods - Encryption
		/// <summary>
		/// PGP-encrypt all data from source stream into destination stream in raw/binary format
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key in ring will be used)</param>
		/// <param name="clearinput">Input stream containing source data to be encrypted</param>
		/// <param name="encryptedoutput">Output stream to receive raw encrypted data</param>
		/// <remarks>
		/// - Source data will be read asynchronously
		/// - Destination data will be written asynchronously
		/// </remarks>
		public static async Task EncryptRaw(Stream publickeysource, string publickeyuserid, Stream clearinput, Stream encryptedoutput)
		{
			// Create encrypted data generator, using public key extracted from provided source:
			var pgpEncDataGen = new PgpEncryptedDataGenerator(SymmetricKeyAlgorithmTag.Aes256, true, new SecureRandom());
			pgpEncDataGen.AddMethod(await ExtractPublicKey(publickeysource, publickeyuserid));

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
		/// PGP-encrypt all data from source to destination stream in ASCII-armored format
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key in ring will be used)</param>
		/// <param name="clearinput">Input stream containing source data to be encrypted</param>
		/// <param name="encryptedoutput">Output stream to receive ASCII-armored encrypted data</param>
		/// <remarks>
		/// - Source data will be read asynchronously
		/// - Destination data will be written asynchronously
		/// </remarks>
		public static async Task Encrypt(Stream publickeysource, string publickeyuserid, Stream clearinput, Stream encryptedoutput)
		{
			// Call raw encryption function with armored output stream wrapping destination:
			using (var armor = new ArmoredOutputStream(encryptedoutput))
			{
				await EncryptRaw(publickeysource, publickeyuserid, clearinput, armor);
			}
		}

		/// <summary>
		/// Build PGP-encrypting (raw/binary format) stream around the provided output stream
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key in ring will be used)</param>
		/// <param name="encryptedoutput">Output stream to receive raw/binary encrypted data</param>
		/// <returns>
		/// A StreamStack object, into which cleartext data can be written (resulting in encrypted data being written to encryptedoutput)
		/// </returns>
		/// <remarks>
		/// - Caller is responsible for disposing of returned StreamStack (BEFORE disposing of original encryptedoutput Stream)
		/// - Caller is also still responsible for disposing of encryptedinput stream (AFTER disposing of returned StreamStack)
		/// </remarks>
		public static async Task<StreamStack> GetEncryptionStreamRaw(Stream publickeysource, string publickeyuserid, Stream encryptedoutput)
		{
			var encryptionstream = new StreamStack();
			try
			{
				// Create encrypted data generator using public key extracted from provided source:
				var pgpEncDataGen = new PgpEncryptedDataGenerator(SymmetricKeyAlgorithmTag.Aes256, true, new SecureRandom());
				pgpEncDataGen.AddMethod(await ExtractPublicKey(publickeysource, publickeyuserid));

				// Create encrypted data generator stream around destination stream and push on to return value stack:
				encryptionstream.PushStream(pgpEncDataGen.Open(encryptedoutput, new byte[1024]));

				// Create compressed data generator stream around encryption stream and push on to return value stack:
				var comData = new PgpCompressedDataGenerator(CompressionAlgorithmTag.Zip);
				encryptionstream.PushStream(comData.Open(encryptionstream.GetStream()));

				// Create literal data generator stream around compression stream and push on to return value stack:
				var lData = new PgpLiteralDataGenerator();
				encryptionstream.PushStream(lData.Open(encryptionstream.GetStream(), PgpLiteralData.Binary, string.Empty, DateTime.UtcNow, new byte[1024]));

				// Return stream object (data written to stream at top of stack will be literalized -> compressed -> encrypted):
				return encryptionstream;
			}
			catch
			{
				encryptionstream.Dispose();
				throw;
			}
		}

		/// <summary>
		/// Build PGP-encrypting (ASCII-armored format) stream around the provided output stream
		/// </summary>
		/// <param name="publickeysource">Input stream containing public keyring bundle</param>
		/// <param name="publickeyuserid">UserID of key to be used within keyring bundle (if null/empty, first available key in ring will be used)</param>
		/// <param name="encryptedoutput">Output stream to receive ASCII-armored encrypted data</param>
		/// <returns>
		/// A StreamStack object, into which cleartext data can be written (resulting in encrypted data being written to encryptedoutput)
		/// </returns>
		/// <remarks>
		/// - Caller is responsible for disposing of returned StreamStack (BEFORE disposing of original encryptedoutput Stream)
		/// - Caller is also still responsible for disposing of encryptedinput stream (AFTER disposing of returned StreamStack)
		/// </remarks>
		public static async Task<StreamStack> GetEncryptionStream(Stream publickeysource, string publickeyuserid, Stream encryptedoutput)
		{
			var encryptionstream = new StreamStack();
			try
			{
				// Create armored output stream wrapping destination stream and push on to return value stack:
				encryptionstream.PushStream(new ArmoredOutputStream(encryptedoutput));

				// Create encrypted data generator using public key extracted from provided source:
				var pgpEncDataGen = new PgpEncryptedDataGenerator(SymmetricKeyAlgorithmTag.Aes256, true, new SecureRandom());
				pgpEncDataGen.AddMethod(await ExtractPublicKey(publickeysource, publickeyuserid));

				// Create encrypted data generator stream around armored output stream and push on to return value stack:
				encryptionstream.PushStream(pgpEncDataGen.Open(encryptionstream.GetStream(), new byte[1024]));

				// Create compressed data generator stream around encryption stream and push on to return value stack:
				var comData = new PgpCompressedDataGenerator(CompressionAlgorithmTag.Zip);
				encryptionstream.PushStream(comData.Open(encryptionstream.GetStream()));

				// Create literal data generator stream around compression stream and push on to return value stack:
				var lData = new PgpLiteralDataGenerator();
				encryptionstream.PushStream(lData.Open(encryptionstream.GetStream(), PgpLiteralData.Binary, string.Empty, DateTime.UtcNow, new byte[1024]));

				// Return stream object (data written to top stream will be literalized -> compressed -> encrypted -> armored):
				return encryptionstream;
			}
			catch
			{
				encryptionstream.Dispose();
				throw;
			}
		}
		#endregion

		#region Public functions - Decryption
		/// <summary>
		/// PGP-decrypt all data from source stream into destination stream
		/// </summary>
		/// <param name="privatekeysource">Input stream containing private keyring bundle</param>
		/// <param name="privatekeypassphrase">Passphrase to access private key (if required)</param>
		/// <param name="encryptedinput">Input stream containing source data to be decrypted</param>
		/// <param name="clearoutput">Output stream to receive decrypted data</param>
		/// <remarks>
		/// Destination data stream will be written asynchronously, but source data stream will be read synchronously (due to limitation in PGP
		/// library; all source data must be available to perform decryption, but source library does not provide async option). If this is a concern,
		/// caller should consider asynchronously pulling source data into local stream first and passing local stream to this function (if increased
		/// use of memory outweighs waiting for blocked thread to perform I/O)
		/// </remarks>
		public static async Task Decrypt(Stream privatekeysource, string privatekeypassphrase, Stream encryptedinput, Stream clearoutput)
		{
			// Create object factory using DecoderStream to read PGP data from source, use factory to extract encrypted data:
			using (var decoder = PgpUtilities.GetDecoderStream(encryptedinput))
			{
				var encrypteddata = new PgpObjectFactory(decoder).GetEncryptedData() ?? throw new ArgumentException("No PGP-encrypted data found");

				// Extract private key from key source (using key ID required by encrypted data object), use to open clear stream:
				using (var clearsource = encrypteddata.GetDataStream(await ExtractPrivateKey(privatekeysource, privatekeypassphrase, encrypteddata.KeyId)))
				{
					// Create new object factory from clear stream and use to write cleartext data to destination:
					await new PgpObjectFactory(clearsource).WriteClearData(clearoutput);
				}
			}
		}

		/// <summary>
		/// Create PGP-decrypting stream around a PGP-encrypted input source stream
		/// </summary>
		/// <param name="privatekeysource">Input stream containing private keyring bundle</param>
		/// <param name="privatekeypassphrase">Passphrase to access private key (if required)</param>
		/// <param name="encryptedinput">Input stream containing source data to be decrypted</param>
		/// <returns>
		/// A StreamStack object, from which decrypted data can be read
		/// </returns>
		/// <remarks>
		/// - Caller is responsible for disposing of returned StreamStack (BEFORE disposing of original encryptedinput Stream)
		/// - Caller is also still responsible for disposing of encryptedinput stream (AFTER disposing of returned StreamStack)
		/// - Source data stream will be read synchronously (due to limitation in PGP library; all source data must be available to perform decryption,
		/// but source library does not provide async option). If this is a concern, caller should consider asynchronously pulling source data into local
		/// stream first and passing local stream to this function (if increased use of memory outweighs waiting for blocked thread to perform I/O)
		/// </remarks>
		public static async Task<StreamStack> GetDecryptionStream(Stream privatekeysource, string privatekeypassphrase, Stream encryptedinput)
		{
			var decryptionstream = new StreamStack();
			try
			{
				// Open decoder stream and push on to return value stack:
				decryptionstream.PushStream(PgpUtilities.GetDecoderStream(encryptedinput));

				// Create object factory (using stream at top of stack) to extract encrypted data:
				var factory = new PgpObjectFactory(decryptionstream.GetStream());
				var encrypteddata = factory.GetEncryptedData() ?? throw new ArgumentException("No PGP-encrypted data found");

				// Extract private key from key source (using key ID required by encrypted data object), use to open
				// clear stream and push on to return value stack:
				decryptionstream.PushStream(encrypteddata.GetDataStream(await ExtractPrivateKey(privatekeysource, privatekeypassphrase, encrypteddata.KeyId)));

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
		private static async Task<PgpPrivateKey> ExtractPrivateKey(Stream keysource, string passphrase, long keyID)
		{
			try
			{
				// Create MemoryStream and copy in contents of keysource (first resetting position of keysource stream, if possible);
				// this is required to support caller reusing private key source stream, if desired (because DecoderStream, when
				// itself disposed, will dispose of its underlying stream and destroy original key data):
				using var memkeysource = new MemoryStream();
				if (keysource.CanSeek)
				{
					keysource.Position = 0;
				}
				await keysource.CopyToAsync(memkeysource);
				memkeysource.Position = 0;

				// Use DecoderStream to read secret keyring bundle from memory stream, retrieve the secret
				// key based on requested keyID, and extract using provided passphrase (if any):
				using var decoderstream = PgpUtilities.GetDecoderStream(memkeysource);
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
		private static async Task<PgpPublicKey> ExtractPublicKey(Stream keysource, string userid)
		{
			// Create MemoryStream and copy in contents of keysource (first resetting position of keysource stream, if possible);
			// this is required to support caller reusing public key source stream, if desired (because DecoderStream, when
			// itself disposed, will dispose of its underlying stream and destroy original key data):
			using var memkeysource = new MemoryStream();
			if (keysource.CanSeek)
			{
				keysource.Position = 0;
			}
			await keysource.CopyToAsync(memkeysource);
			memkeysource.Position = 0;

			// Use DecoderStream to read public keyring bundle from memory stream:
			using var decoderstream = PgpUtilities.GetDecoderStream(memkeysource);
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
		public static async Task WriteClearData(this PgpObjectFactory factory, Stream cleardest)
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
