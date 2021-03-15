using System;
using System.IO;
using System.Net.Mail;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace fiQ.TaskUtilities
{
	/// <summary>
	/// Utility class for wrapping SMTP client and performing simple outbound email sending
	/// </summary>
	/// <remarks>
	/// Requires an SmtpOptions configuration and logger having been registered in ServiceCollection
	/// </remarks>
	public class Smtp : IDisposable
	{
		#region Fields and constructors
		private static readonly char[] separators = new char[] { ',', ';' };
		private readonly SmtpOptions options;
		private readonly ILogger logger;
		private bool disposed = false;
		private SmtpClient smtpClient = null;

		public Smtp(IOptions<SmtpOptions> _options, ILogger<Smtp> _logger)
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
		/// Method to ensure that SmtpClient has been initialized
		/// </summary>
		public void EnsureValid()
		{
			if (smtpClient == null)
			{
				throw new InvalidOperationException("SmtpClient not initialized (configuration may be missing)");
			}
		}

		/// <summary>
		/// Send simple email message
		/// </summary>
		public async Task<bool> SendEmail(string messageSubject, string messageBody, string messageTo = null, string messageFrom = null,
			Stream attachment = null, string attachmentName = null)
		{
			/*try // TODO: UNCOMMENT EMAIL
			{
				EnsureValid();
				using (var mailMessage = new MailMessage
				{
					From = new MailAddress(string.IsNullOrEmpty(messageFrom) ? options.DefaultFrom : messageFrom),
					Subject = messageSubject,
					Body = messageBody,
					IsBodyHtml = true
				})
				{
					mailMessage.To.Add(FormatToList(string.IsNullOrEmpty(messageTo) ? options.DefaultTo : messageTo));
					if (attachment != null)
					{
						mailMessage.Attachments.Add(new Attachment(attachment, attachmentName));
					}
					await smtpClient.SendMailAsync(mailMessage);
					return true;
				}
			}
			catch (Exception ex)
			{
				if (ex is AggregateException ae)
				{
					ex = TaskUtilities.General.SimplifyAggregateException(ae);
				}
				logger.LogError(ex, "SMTP send failed");
			}*/
			return false;
		}
		#endregion

		#region Private methods
		/// <summary>
		/// Properly format "to" email listing (comma-separated, trimmed with empty entries removed)
		/// </summary>
		private string FormatToList(string to)
		{
			return string.Join(',', to.Split(separators, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
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
