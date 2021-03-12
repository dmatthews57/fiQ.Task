using System;
using System.Collections.Generic;
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

		#region Abstract methods - Files
		public abstract HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths);
		#endregion
	}
}
