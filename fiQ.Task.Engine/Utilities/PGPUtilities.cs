using System;
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
		public static async System.Threading.Tasks.Task EncryptRaw(Stream publickeysource, string publickeyuserid, Stream clearsource, Stream encrypteddest)
		{
			// Create encrypted data generator, using public key extracted from provided source:
			var pgpEncDataGen = new PgpEncryptedDataGenerator(SymmetricKeyAlgorithmTag.Aes256, true, new SecureRandom());
			pgpEncDataGen.AddMethod(ExtractPublicKey(publickeysource, publickeyuserid));

			// Wrap destination stream in encrypted data generator stream:
			using (var encrypt = pgpEncDataGen.Open(encrypteddest, new byte[1024]))
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
						await clearsource.CopyToAsync(literal);
					}
				}
			}
		}
		public static async System.Threading.Tasks.Task Encrypt(Stream publickeysource, string publickeyuserid, Stream clearsource, Stream encrypteddest)
		{
			// Call raw encryption function with armored output stream wrapping destination:
			using (var armor = new ArmoredOutputStream(encrypteddest))
			{
				await EncryptRaw(publickeysource, publickeyuserid, clearsource, armor);
			}
		}

		public static async System.Threading.Tasks.Task Decrypt(Stream privatekeysource, string privatekeypassphrase, Stream encryptedsource, Stream cleardest)
		{
			// Create object factory using DecoderStream to read PGP data from source, use factory to extract encrypted data:
			using var decoder = PgpUtilities.GetDecoderStream(encryptedsource);
			var encrypteddata = new PgpObjectFactory(decoder).GetEncryptedData() ?? throw new ArgumentException("No PGP-encrypted data found");

			// Extract private key from key source (using key ID required by encrypted data object), use to open clear stream:
			using (var clearsource = encrypteddata.GetDataStream(ExtractPrivateKey(privatekeysource, privatekeypassphrase, encrypteddata.KeyId)))
			{
				// Create new object factory from clear stream and use to write cleartext data to destination:
				await new PgpObjectFactory(clearsource).WriteClearData(cleardest);
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
					.FirstOrDefault(key => key.IsEncryptionKey && (string.IsNullOrEmpty(userid) || key.GetUserIds().Cast<string>().Any(userid => userid.Equals(userid, StringComparison.OrdinalIgnoreCase))))
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
		/// combined with PgpPrivateKey) and write payload into destination stream
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
					await cleardata.GetInputStream().CopyToAsync(cleardest);
					return;
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
