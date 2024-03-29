﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace fiQ.TaskAdapters.FileMove
{
	/// <summary>
	/// Abstract base class defining interface for a FileMove connection (simple folder, SFTP, etc)
	/// </summary>
	internal abstract class Connection : IDisposable
	{
		#region Fields and constructor
		protected ConnectionConfig config { get; }
		private MemoryStream pgpKeyStream = null;
		private bool disposed = false;

		protected Connection(ConnectionConfig _config)
		{
			config = _config;
		}
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
		public static Connection CreateInstance(IServiceProvider _isp, ConnectionConfig _config)
		{
			switch (_config.locationType)
			{
				case ConnectionConfig.LocationType.Folder:
					return (Connection)ActivatorUtilities.CreateInstance(_isp, typeof(FolderConnection), _config);
				case ConnectionConfig.LocationType.SFTP:
					return (Connection)ActivatorUtilities.CreateInstance(_isp, typeof(SftpConnection), _config);
				case ConnectionConfig.LocationType.FTP:
					break;
				case ConnectionConfig.LocationType.Email:
					return (Connection)ActivatorUtilities.CreateInstance(_isp, typeof(EmailConnection), _config);
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

		#region Protected abstract methods - Connection management
		protected abstract void DoConnect();
		protected abstract void DoDisconnect();
		#endregion

		#region Abstract methods - File transfer (source mode)
		public abstract HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths);
		public abstract Task DoTransfer(string folderPath, string fileName, Stream writestream);
		public abstract void DeleteFile(string folderPath, string fileName);
		public abstract void SourceRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite);
		#endregion

		#region Abstract methods - File transfer (destination mode)
		public abstract StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite);
		public abstract Task FinalizeWrite(StreamPath streampath);
		public abstract void DestRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite);
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
