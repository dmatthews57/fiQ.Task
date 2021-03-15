using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Abstract base class defining interface for a FileMove connection (simple folder, SFTP, etc)
	/// </summary>
	internal abstract class Connection : IDisposable
	{
		#region Fields
		protected ConnectionConfig config { get; init; }
		private MemoryStream pgpKeyStream = null;
		private bool disposed = false;
		#endregion

		#region IDisposable default implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		protected virtual void Dispose(bool disposing)
		{
			if (disposed == false && disposing && pgpKeyStream != null)
			{
				pgpKeyStream.Dispose();
				pgpKeyStream = null;
			}
			disposed = true;
		}
		#endregion

		#region Public static factory method
		public static Connection CreateInstance(ConnectionConfig _config)
		{
			switch (_config.locationType)
			{
				case ConnectionConfig.LocationType.Folder:
					return new FolderConnection { config = _config };
				case ConnectionConfig.LocationType.SFTP:
					return new SftpConnection { config = _config };
				case ConnectionConfig.LocationType.FTP:
					break;
				case ConnectionConfig.LocationType.Email:
					break;
			};

			throw new ArgumentException("Invalid connection type requested");
		}
		#endregion

		#region Public properties
		/// <summary>
		/// Retrieve stream containing PGP (public or private) key data
		/// </summary>
		public Stream PGPKeyStream
		{
			get
			{
				return pgpKeyStream ?? throw new InvalidOperationException("PGP key stream has not been initialized");
			}
		}
		#endregion

		#region Public methods - Connection management
		public void Connect()
		{
			// If PGP encryption/decryption required, open PGP key file and copy contents into MemoryStream:
			if (config.PGP)
			{
				using (var keyfilestream = new FileStream(config.pgpKeyRing, FileMode.Open, FileAccess.Read, FileShare.Read))
				{
					if (pgpKeyStream == null)
					{
						pgpKeyStream = new MemoryStream();
					}
					else
					{
						pgpKeyStream.SetLength(0);
					}
					keyfilestream.CopyTo(pgpKeyStream);
				}
			}

			// Call abstract DoConnect function:
			DoConnect();
		}
		public void Disconnect() => DoDisconnect();
		#endregion

		#region Abstract methods - File transfer
		public abstract HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths);
		public abstract StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite);
		public abstract Task DoTransfer(string folderPath, string fileName, Stream writestream);
		#endregion

		#region Abstract methods - File management
		public abstract void RenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite);
		public abstract void DeleteFile(string folderPath, string fileName);
		#endregion

		#region Virtual methods - Simple method file transfer (default not supported)
		/// <summary>
		/// Function to indicate whether this connection supports simple copy operation from specified source
		/// </summary>
		/// <remarks>
		/// Any child class that overrides this function should also override DoSimpleCopy
		/// </remarks>
		public virtual bool SupportsSimpleCopy(Connection sourceConnection)
		{
			return false;
		}
		/// <summary>
		/// Function to perform simple copy, if supported
		/// </summary>
		/// <remarks>
		/// Any child class that overrides this function should also override SupportsSimpleCopy
		/// </remarks>
		/// <returns>Path to destination file</returns>
		public virtual string DoSimpleCopy(Connection sourceConnection, string sourceFolderPath, string sourceFileName,
			string destFolderPath, string destFileName, bool preventOverwrite)
		{
			throw new NotImplementedException("Simple copy not supported");
		}
		#endregion

		#region Protected abstract methods - Connection management
		protected abstract void DoConnect();
		protected abstract void DoDisconnect();
		#endregion
	}
}
