using System.Text.RegularExpressions;
using fiQ.TaskModels;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Container class for configuration of a specific source or destination connection
	/// </summary>
	internal class ConnectionConfig
	{
		public enum LocationType
		{
			Invalid,
			Folder,
			SFTP,
			FTP,
			Email
		};

		#region Private fields
		private static Regex REGEX_SFTP_SERVER = new Regex(@"^[sS][fF][tT][pP]\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(\:\d+)?(\/)?");
		private static Regex REGEX_FTP_SERVER = new Regex(@"^[fF][tT][pP][sS]?\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z])*(\:\d+)?(\/)?");
		#endregion

		public LocationType locationType { get; init; } = LocationType.Invalid;
		public string location { get; init; }

		#region Credentials
		public string userID { get; init; }
		public string password { get; init; }
		public string clientCertificate { get; init; }
		#endregion

		#region PGP configuration
		public string pgpKeyRing { get; init; } = null; // Private keyring for decryption if source connection, public keyring for encryption if destination connection
		public string pgpPassphrase { get; init; } = null; // Passphrase for secret key (source connection only)
		public string pgpUserID { get; init; } = null; // UserID for key to be used from public key ring (destination connection only; if not specified, first available key will be used)
		public bool pgpRawFormat { get; init; } = false; // If using PGP encryption, specifies raw format (destination connection only; default is ASCII-armored)
		#endregion

		public ConnectionConfig(TaskParameters parameters, bool source)
		{
			location = parameters.GetString(source ? "SourceLocation" : "DestinationLocation");

			#region Determine location type based on location string
			if (!string.IsNullOrEmpty(location))
			{
				if (REGEX_SFTP_SERVER.IsMatch(location))
				{
					locationType = LocationType.SFTP;
					if (!location.EndsWith('/')) // Ensure URI ends with forward slash
					{
						location += "/";
					}
				}
				else if (REGEX_FTP_SERVER.IsMatch(location))
				{
					locationType = LocationType.FTP;
					if (!location.EndsWith('/')) // Ensure URI ends with forward slash
					{
						location += "/";
					}
				}
				else if (TaskUtilities.General.REGEX_EMAIL.IsMatch(location))
				{
					locationType = LocationType.Email;
				}
				else if (TaskUtilities.General.REGEX_DIRPATH.IsMatch(location))
				{
					locationType = LocationType.Folder;
				}
			}
			#endregion

			#region Retrieve credentials
			userID = parameters.GetString(source ? "SourceUserID" : "DestinationUserID");
			password = parameters.GetString(source ? "SourcePassword" : "DestinationPassword");
			clientCertificate = parameters.GetString(source ? "SourceClientCert" : "DestinationClientCert");
			#endregion

			#region Retrieve PGP variables
			pgpKeyRing = parameters.GetString(source ? "PGPPrivateKeyRing" : "PGPPublicKeyRing");
			if (!string.IsNullOrEmpty(pgpKeyRing))
			{
				if (source) // Source connection means we are retrieving files, and will be decrypting using private key
				{
					pgpPassphrase = parameters.GetString("PGPPassphrase");
				}
				else // Destination connection means we are pushing files, and will be encrypting using public key
				{
					pgpUserID = parameters.GetString("PGPUserID");
					pgpRawFormat = parameters.GetBool("PGPRawFormat");
				}
			}
			#endregion
		}

		#region Public properties
		public bool Invalid
		{
			get
			{
				return locationType == LocationType.Invalid || string.IsNullOrEmpty(location);
			}
		}
		public bool PGP
		{
			get
			{
				return !string.IsNullOrEmpty(pgpKeyRing);
			}
		}
		#endregion
	}
}
