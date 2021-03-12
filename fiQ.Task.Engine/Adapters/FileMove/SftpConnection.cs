using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Renci.SshNet;

namespace fiQ.TaskAdapters.FileMove
{
	class SftpConnection : Connection
	{
		#region Fields
		private Uri uri = null;
		private SftpClient sftpClient = null;
		private bool disposed = false;
		#endregion

		#region IDisposable implementation
		protected override void Dispose(bool disposing)
		{
			if (disposed == false && disposing && sftpClient != null)
			{
				sftpClient.Dispose();
				sftpClient = null;
			}
			disposed = true;
			base.Dispose(disposing);
		}
		#endregion

		#region Connection implementation - Connection management
		/// <summary>
		/// Connection function - create SftpClient and connect to server
		/// </summary>
		public override void Connect()
		{
			// SftpClient will throw unfriendly error for this specific case; check first:
			if (string.IsNullOrEmpty(config.userID))
			{
				throw new ArgumentException("SFTP UserID required");
			}

			// Create Uri from configuration and create client:
			uri = new Uri(config.location);
			sftpClient = TaskUtilities.Sftp.ConnectSftpClient(uri, config.userID, config.password, config.clientCertificate);

			// If base path includes a subfolder, change directory now:
			var ap = uri.AbsolutePath;
			if (ap.StartsWith("/"))
			{
				// Remove leading slash so path is by default relative to home folder - URI can be configured with "//" in order
				// to force path from root folder, if required (i.e. sftp://host/relativetohome vs sftp://host//relativetoroot")
				ap = ap.Substring(1);
			}
			if (!string.IsNullOrEmpty(ap))
			{
				sftpClient.ChangeDirectory(ap);
			}
		}
		/// <summary>
		/// Disconnection function - gracefully disconnect from server, if connected
		/// </summary>
		public override void Disconnect()
		{
			if (sftpClient != null)
			{
				sftpClient.Disconnect();
			}
		}
		#endregion

		#region Connection implementation - Files
		/// <summary>
		/// Retrieve listing of downloadable files
		/// </summary>
		/// <param name="paths">Collection of SourceFilePaths to search</param>
		/// <returns>HashSet of downloadable files in source paths, matching source </returns>
		public override HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths)
		{
			var fileset = new HashSet<DownloadFile>();
			foreach (var path in paths)
			{
				// Construct Regex for filename filter (required), and for filename regex (optional):
				var fileFilterRegex = TaskUtilities.General.RegexFromFileFilter(path.FilenameFilter);
				var fileNameRegex = TaskUtilities.General.RegexIfPresent(path.FilenameRegex, RegexOptions.IgnoreCase);

				fileset.UnionWith(
					sftpClient.ListDirectory(string.IsNullOrEmpty(path.FolderPath) ? "." : path.FolderPath)
						.Where(sf =>
							sf.IsDirectory == false
							&& fileFilterRegex.IsMatch(sf.Name)
							&& (fileNameRegex == null ? true : fileNameRegex.IsMatch(sf.Name))
						)
						.Select(sf => new DownloadFile(path)
						{
							fileName = sf.Name,
							lastWriteTime = sf.LastWriteTime,
							size = sf.Length
						})
				);
			}
			return fileset;
		}
		#endregion
	}
}
