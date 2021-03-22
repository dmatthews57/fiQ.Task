using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace fiQ.TaskAdapters.FileMove
{
	class EmailConnection : Connection
	{
		#region Fields and constructor
		private readonly TaskUtilities.Smtp smtp;
		public EmailConnection(TaskUtilities.Smtp _smtp, ConnectionConfig _config) : base(_config)
		{
			smtp = _smtp;
		}
		#endregion

		#region Connection implementation - Connection management
		/// <summary>
		/// Connection function - Ensure SMTP client is valid
		/// </summary>
		protected override void DoConnect()
		{
			smtp.EnsureValid();
		}
		/// <summary>
		/// Disconnection function - currently unused
		/// </summary>
		protected override void DoDisconnect()
		{
		}
		#endregion

		#region Connection implementation - File transfer (source mode)
		/// <summary>
		/// Retrieve listing of downloadable files
		/// </summary>
		public override HashSet<DownloadFile> GetFileList(List<SourceFilePath> paths)
		{
			throw new InvalidOperationException("Email not valid as source connection");
		}
		/// <summary>
		/// Perform transfer of data from file at specified path to destination stream
		/// </summary>
		public override async Task DoTransfer(string folderPath, string fileName, Stream writestream)
		{
			throw new InvalidOperationException("Email not valid as source connection");
		}
		/// <summary>
		/// Delete specified file
		/// </summary>
		public override void DeleteFile(string folderPath, string fileName)
		{
			throw new InvalidOperationException("Email not valid as source connection");
		}
		/// <summary>
		/// Rename specified file at source
		/// </summary>
		public override void SourceRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite)
		{
			throw new InvalidOperationException("Email not valid as source connection");
		}
		#endregion

		#region Connection implementation - File transfer (destination mode)
		/// <summary>
		/// Open writable stream for specified destination file (creates MemoryStream to hold data until FinalizeWrite is called)
		/// </summary>
		/// <remarks>Used only when this is "destination" connection</remarks>
		public override StreamPath GetWriteStream(string folderPath, string fileName, bool preventOverwrite)
		{
			return new StreamPath
			{
				stream = new MemoryStream(),
				path = fileName
			};
		}
		/// <summary>
		/// Perform cleanup/finalization steps on StreamPath
		/// </summary>
		/// <remarks>
		/// MemoryStream previously created by GetWriteStream should now be populated with data from source file, and
		/// can now be used to stream attachment to SMTP server
		/// </remarks>
		public override async Task FinalizeWrite(StreamPath streampath)
		{
			streampath.stream.Position = 0; // Rewind MemoryStream to start of data
			await smtp.SendEmail(messageSubject: streampath.path,
				messageBody: "Your file is attached",
				messageTo: config.location,
				attachment: streampath.stream,
				attachmentName: streampath.path);
		}
		/// <summary>
		/// Perform deferred rename of file at destination (no action required, ignore)
		/// </summary>
		public override void DestRenameFile(string folderPath, string fileName, string newFileName, bool preventOverwrite)
		{
		}
		#endregion
	}
}
