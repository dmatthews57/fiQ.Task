﻿using System;
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
		#endregion

		#region IDisposable default implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		protected virtual void Dispose(bool disposing)
		{
			// Child classes should override if they contain disposable members
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

		#region Abstract methods - Connection management
		public abstract void Connect();
		public abstract void Disconnect();
		#endregion

		#region Abstract methods - File transfer
		public abstract HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths);
		public abstract StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite);
		public abstract Task DoTransfer(string folderPath, string fileName, Stream writestream);
		#endregion

		#region Abstract methods - File management
		public abstract void RenameFile(string folderPath, string fileName, string newFileName);
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
	}
}
