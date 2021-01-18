using System;
using System.IO;
using System.Net.Mail;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace fiQ.Task.Utilities
{
	/// <summary>
	/// Utility class for wrapping SMTP client and performing simple outbound email sending
	/// </summary>
	/// <remarks>
	/// Requires an SmtpOptions configuration and logger having been registered in ServiceCollection
	/// </remarks>
	public class SmtpUtilities : IDisposable
	{
		#region Fields and constructors
		private readonly SmtpOptions options;
		private readonly ILogger logger;
		private bool disposed = false;
		private SmtpClient smtpClient = null;

		public SmtpUtilities(IOptions<SmtpOptions> _options, ILogger<SmtpUtilities> _logger)
		{
			options = _options?.Value;
			logger = _logger;

			// If valid configuration provided, create SmtpClient
			if (!string.IsNullOrEmpty(options?.Host) && options?.Port > 0)
			{
				smtpClient = new SmtpClient
				{
					Host = options.Host,
					Port = (int)options.Port,
					Credentials = string.IsNullOrEmpty(options.UserID) ? null : new System.Net.NetworkCredential(options.UserID, options.Password),
					EnableSsl = options.TLS
				};
			}
		}
		#endregion

		#region IDisposable implementation
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
		protected virtual void Dispose(bool disposing)
		{
			if (disposed == false && disposing && smtpClient != null)
			{
				smtpClient.Dispose();
				smtpClient = null;
			}
			disposed = true;
		}
		#endregion

		#region Public methods
		/// <summary>
		/// Send simple email message
		/// </summary>
		public bool SendEmail(string messageSubject, string messageBody, string messageTo = null, string messageFrom = null,
			Stream attachment = null, string attachmentName = null)
		{
			Console.WriteLine($"Sending to {messageTo}:\n{messageSubject}\n{messageBody}");

			/*
			try
			{
				if (smtpClient == null) throw new InvalidOperationException("Client not initialized (configuration may be missing)");
				using (var mailMessage = new MailMessage
				{
					From = new MailAddress(string.IsNullOrEmpty(messageFrom) ? options.DefaultFrom : messageFrom),
					Subject = messageSubject,
					Body = messageBody,
					IsBodyHtml = true
				})
				{
					mailMessage.To.Add((string.IsNullOrEmpty(messageTo) ? options.DefaultTo : messageTo).Replace(";", ","));
					if (attachment != null)
					{
						mailMessage.Attachments.Add(new Attachment(attachment, attachmentName));
					}
					smtpClient.Send(mailMessage);
					return true;
				}
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "SMTP send failed");
			}
			*/
			return false;
		}
		#endregion
	}

	#region Configuration options class
	public class SmtpOptions
	{
		public string Host { get; set; } = null;
		public int? Port { get; set; } = null;
		public bool TLS { get; set; } = false;
		public string UserID { get; set; } = null;
		public string Password { get; set; } = null;
		public string DefaultFrom { get; set; } = null;
		public string DefaultTo { get; set; } = null;
	}
	#endregion
}
