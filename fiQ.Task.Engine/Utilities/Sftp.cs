using System;
using Renci.SshNet;

namespace fiQ.TaskUtilities
{
	public static class Sftp
	{
		public static SftpClient ConnectSftpClient(Uri hostUri, string userID, string password, string clientCert = null)
		{
			// Declare disposable values in case of exception:
			SftpClient sftpClient = null;
			PrivateKeyFile privateKeyFile = null;
			var methods = new AuthenticationMethod[1];
			try
			{
				// Populate authentication method array with password or private key method:
				if (string.IsNullOrEmpty(clientCert))
				{
					methods[0] = new PasswordAuthenticationMethod(userID ?? string.Empty, password ?? string.Empty);
				}
				else
				{
					privateKeyFile = new PrivateKeyFile(clientCert, password);
					methods[0] = new PrivateKeyAuthenticationMethod(userID ?? string.Empty, new[] { privateKeyFile });
				}

				// Create SftpClient, attempt connection and return:
				sftpClient = new SftpClient(new ConnectionInfo(hostUri.Host, (hostUri.Port == -1 ? 22 : hostUri.Port), userID, methods));
				sftpClient.Connect();
				return sftpClient;
			}
			catch
			{
				if (sftpClient != null)
				{
					sftpClient.Dispose();
				}
				throw;
			}
			finally
			{
				if (privateKeyFile != null)
				{
					privateKeyFile.Dispose();
				}
				if (methods[0] is IDisposable disposable)
				{
					disposable.Dispose();
				}
			}
		}
	}
}
