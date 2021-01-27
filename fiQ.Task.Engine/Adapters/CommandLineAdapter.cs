using System;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using fiQ.Task.Models;
using fiQ.Task.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace fiQ.Task.Adapters
{
	/// <summary>
	/// TaskAdapter to execute another program via command line
	/// </summary>
	public class CommandLineAdapter : TaskAdapter
	{
		#region Fields and constructors
		public CommandLineAdapter(IConfiguration _config, ILogger<CommandLineAdapter> _logger, string taskName = null)
			: base(_config, _logger, taskName) { }
		#endregion

		/// <summary>
		/// Execute ...
		/// </summary>
		public override async Task<TaskResult> ExecuteTask(TaskParameters parameters)
		{
			var result = new TaskResult();
			var consoleOutput = new StringBuilder();
			var dateTimeNow = DateTime.Now; // Use single value throughout for consistency in macro replacements
			try
			{
				#region Retrieve task parameters
				// Retrieve and validate required parameters
				string executable = parameters.GetString("Executable");
				if (string.IsNullOrEmpty(executable))
				{
					throw new ArgumentException("Missing Executable name");
				}

				// Retrieve optional parameters
				string workingFolder = parameters.GetString("WorkingFolder", TaskUtilities.REGEX_DIRPATH, dateTimeNow);
				string returnValueRegex = parameters.GetString("ReturnValueRegex");
				#endregion

				#region Create and execute command line process
				try
				{
					using (var process = new Process())
					{
						process.StartInfo.FileName = "cmd.exe";
						process.StartInfo.WorkingDirectory = string.IsNullOrEmpty(workingFolder) ? Directory.GetCurrentDirectory() : workingFolder;

						// Construct command string, starting with executable name:
						var commandstring = new StringBuilder(executable);

						#region Add additional command line arguments from parameter collection
						// Supports up to 10 arguments, named as "argument0" through "argument9":
						var argumentKeyRegex = new Regex(@"^Argument\d$", RegexOptions.IgnoreCase);
						var argumentkeys = parameters.GetKeys()
							.Where(argumentkey => argumentKeyRegex.IsMatch(argumentkey))
							.OrderBy(argumentkey => argumentkey);
						if (argumentkeys.Any())
						{
							// Set up regex to capture argument-named macros, to allow values passed as parameters to this adapter
							// (including return values output by previous adapters in batch) to be placed directly into command string; for
							// example if a previous adapter in this batch output return value "@FileID"/57, placing the parameter
							// "Argument0"/"-FileID=<@@FileID>" in the collection for THIS adapter will result in value "-FileID=57" being
							// placed in the command line to be executed:
							var argumentValueMacroRegex = new Regex(@"<@(?<name>[^>]+)>");
							foreach (string argumentkey in argumentkeys)
							{
								// First retrieve the specific argument by key name, processing date macros:
								string argumentvalue = parameters.GetString(argumentkey, null, dateTimeNow);

								// Now process any nested macros in resulting argument value string, flattening match collection
								// into name/value pair and selecting unique values only:
								var argumentvaluemacromatches = argumentValueMacroRegex.Matches(argumentvalue)
									.OfType<Match>()
									.Select(match => new { Name = match.Groups["name"].Value, Value = match.Value })
									.Distinct();
								foreach (var match in argumentvaluemacromatches)
								{
									// Retrieve parameter matching the "name" portion of the macro - processing date/time macros
									// again - and replace all instances of the specified macro with the string retrieved:
									argumentvalue = argumentvalue.Replace(match.Value, parameters.GetString(match.Name, null, dateTimeNow));
								}

								// Add final argument value to command line:
								commandstring.Append($" {argumentvalue}");
							}
						}
						#endregion

						// Construct cmd.exe arguments starting with /c (to trigger process exit once execution of command is
						// complete), adding command string in quotes (escaping any quotes within string itself), and ending by
						// redirecting stderr to stdout (so all console output will be read in chronological order):
						var finalcommandstring = commandstring.ToString();
						process.StartInfo.Arguments = $"/c \"{Regex.Replace(finalcommandstring, "\\\\?[\"]", "\\\"")}\" 2>&1";
						logger.LogDebug($"Executing [{finalcommandstring}]");

						// Don't execute command inside shell (directly execute cmd.exe process), capture console output to streams:
						process.StartInfo.UseShellExecute = false;
						process.StartInfo.RedirectStandardOutput = true;
						process.StartInfo.RedirectStandardError = true;

						// Add event handler for console output data - adds any streamed output to StringBuilder, flags task completion
						// when streams close (null data received):
						var outputCompletionTask = new TaskCompletionSource<bool>();
						process.OutputDataReceived += (sender, e) =>
						{
							if (e.Data == null)
							{
								outputCompletionTask.TrySetResult(true);
							}
							else
							{
								consoleOutput.AppendLine(e.Data);
							}
						};

						// Set up event processing for process exit (return process return value via Task object):
						process.EnableRaisingEvents = true;
						var processCompletionTask = new TaskCompletionSource<int>();
						process.Exited += (sender, e) =>
						{
							processCompletionTask.TrySetResult(process.ExitCode);
						};

						// Launch process, begin asynchronous reading of output:
						process.Start();
						process.BeginOutputReadLine();

						// Wait for process to exit, then wait on output handles to close (to ensure all console output is read
						// and streams are properly cleaned up):
						int returnValue = await processCompletionTask.Task.ConfigureAwait(false);
						await outputCompletionTask.Task.ConfigureAwait(false);

						// If configuration does not specify a return value validation regex, assume success:
						if (string.IsNullOrEmpty(returnValueRegex) ? true : Regex.IsMatch(returnValue.ToString(), returnValueRegex))
						{
							logger.LogInformation($"Process exited with code {returnValue}");
							result.Success = true;
						}
						else
						{
							throw new Exception($"Invalid process exit code ({returnValue})");
						}
					}
				}
				catch (AggregateException ae) // Catches asynchronous exceptions only
				{
					throw TaskUtilities.SimplifyAggregateException(ae);
				}
				#endregion
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Command execution failed");
				result.AddException(ex);
			}

			// If console output was captured, log now:
			if (consoleOutput.Length > 0)
			{
				logger.LogDebug(consoleOutput.ToString());
			}
			return result;
		}
	}
}
