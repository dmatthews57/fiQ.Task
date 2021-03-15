using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using fiQ.TaskModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

namespace fiQ.TaskCmd
{
	class Program
	{
		static void Main(string[] args)
		{
			TaskEngineConfig engineConfig = new TaskEngineConfig(); // Engine execution parameter collection
			string jobName = null; // For logging purposes
			string taskFileName = null; // For running a single file
			string taskFolderName = null; // For running all files in a folder
			var cmdlineTaskParameters = new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase); // Global overrides for task parameters

			#region Load application configuration
			var env = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");
			var configbuilder = new ConfigurationBuilder()
				.SetBasePath(Directory.GetCurrentDirectory())
				.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
				.AddJsonFile($"appsettings.{env}.json", optional: true, reloadOnChange: false)
				.AddEnvironmentVariables();
			if (env == "Development")
			{
				configbuilder.AddUserSecrets<Program>();
			}
			if (args != null)
			{
				var configargs = new List<string>(); // Store unrecognized/generic parameters

				#region Parse command-line arguments
				// Name/value pairs with an "@" prefix are TaskParameter override values:
				var REGEX_TASKPARM = new Regex(@"^@(?<name>\S+)[=](?<value>.+)$");
				// Other name/value pairs (optionally starting with any of "-", "--" or "/") are program-level:
				var REGEX_NAMEDPARM = new Regex(@"^(--|-|/)?(?<name>\S+)[=](?<value>.+)$");
				var REGEX_NAMEDPARM_SHORTKEY = new Regex(@"^-(?![-])");
				// Flag-only parameters (i.e. not a name/value pair):
				var REGEX_FLAGPARM = new Regex(@"^(--|-|/)?(?<value>[\S-[=]]+)$");
				// Regex to capture and strip argument prefix:
				var REGEX_PARMLEAD = new Regex(@"^(--|-|/)");

				// Iterate through all arguments, looking for recognized values (may short-circuit normal
				// configuration behavior to allow custom parameters for this application or any adapter)
				foreach (var arg in args)
				{
					// Check for task parameter override value first
					var match = REGEX_TASKPARM.Match(arg);
					if (match.Success)
					{
						cmdlineTaskParameters[$"Parameters:{match.Groups["name"].Value}"] = match.Groups["value"].Value;
					}
					else
					{
						// Check for flag-only parameter:
						match = REGEX_FLAGPARM.Match(arg);
						if (match.Success)
						{
							// Check for reserved configuration flag values; if not one of the specified supported
							// options, add to configuration collection (DI clients may recognize/use it):
							switch (match.Groups["value"].Value.ToLowerInvariant())
							{
								case "console": // Enable console logging
									// Add console sink to Serilog config (added at node "99" to prevent overwriting
									// any existing sinks; note that if a console sink is already configured, this
									// will result in messages being output to console twice)
									configargs.Add("Serilog:WriteTo:99:Name=Console");
									break;
								case "debug": // Set minimum logging level to Debug
									configargs.Add("Serilog:MinimumLevel=Debug");
									break;
								case "haltonerror": // Halt batch of tasks if any individual step fails
									engineConfig.HaltOnError = true;
									break;
								case "debugreturn": // Dump return values at end of execution to debug log
									engineConfig.DebugReturnValues = true;
									break;
								default: // Unrecognized value - add to config collection
									configargs.Add(arg);
									break;
							};
						}
						else
						{
							// Check for named parameter:
							match = REGEX_NAMEDPARM.Match(arg);
							if (match.Success)
							{
								// Check for reserved configuration names; if not one of the specified supported
								// options, add to configuration collection (DI clients may recognize/use it):
								switch (match.Groups["name"].Value.ToLowerInvariant())
								{
									case "jobname": // Name of job, for logging purposes
										jobName = match.Groups["value"].Value;
										break;
									case "taskfile": // Name of specific task file to execute
										taskFileName = match.Groups["value"].Value;
										break;
									case "taskfolder": // Name of folder of task files to execute
										taskFolderName = match.Groups["value"].Value;
										// Use folder name as default jobName, unless already provided:
										if (string.IsNullOrEmpty(jobName))
										{
											jobName = taskFolderName;
										}
										break;
									default: // Unrecognized parameter - add to config collection
										// (replace single leading "-" with "--", as MS config provider will
										// hysterically throw a formatting exception for unmapped "short keys")
										configargs.Add(REGEX_NAMEDPARM_SHORTKEY.Replace(arg, "--"));
										break;
								};
							}
							else
							{
								// Argument is in unrecognized format, just add to configuration collection:
								configargs.Add(arg);
							}
						}
					}
				}
				// If any arguments were added to configuration collection, add to ConfigBuilder now:
				if (configargs.Count > 0)
				{
					configbuilder.AddCommandLine(configargs.ToArray());
				}
				#endregion
			}
			var config = configbuilder.Build();
			#endregion

			// Build logger configuration from application config file and create logger:
			Log.Logger = new LoggerConfiguration()
				.ReadFrom.Configuration(config)
				.Enrich.WithProperty("AppVer", typeof(Program).Assembly.GetName().Version)
				.Enrich.WithProperty("LibVer", typeof(TaskEngine).Assembly.GetName().Version)
				.CreateLogger();

			// Set up services and configurations for dependency injection:
			var serviceCollection = new ServiceCollection()
				.AddLogging(configure => configure.AddSerilog(dispose: true))
				.AddMemoryCache()
				.AddSingleton<IConfiguration>(config)
				.Configure<TaskUtilities.SmtpOptions>(config.GetSection("Smtp"))
				.AddTransient<TaskUtilities.Smtp>()
				.AddSingleton(x => ActivatorUtilities.CreateInstance<TaskEngine>(x, jobName))
			;

			using (var serviceProvider = serviceCollection.BuildServiceProvider())
			{
				var adapterConfigs = new List<TaskParameters>();
				var logMessage = new StringBuilder();

				#region Create collection of adapter configurations from incoming parameters
				try
				{
					IOrderedEnumerable<string> configFiles = null;
					if (!string.IsNullOrEmpty(taskFolderName))
					{
						// Running all adapter files in folder; retrieve list of JSON files:
						configFiles = Directory.EnumerateFiles(taskFolderName, "*.json")
							// Ensure file extension is actually JSON (avoid false-positives with 8.3 filenames):
							.Where(filename => filename.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
							// Sort files in alphabetical order, to allow deliberate ordering of steps:
							.OrderBy(filename => filename);
					}
					else if (!string.IsNullOrEmpty(taskFileName))
					{
						// Running single task file only - create collection with single entry:
						configFiles = new List<string>() { taskFileName }.OrderBy(x => x);
					}
					// (else leave configFiles null, and fall through to error logic below)

					#region Iterate through all files, constructing parameter objects
					if (configFiles != null)
					{
						foreach (var configFile in configFiles)
						{
							try
							{
								// Build configuration object from Json file (combined with override parameters
								// from command line), bind to a TaskParameterTemplate object:
								var taskconfig = new ConfigurationBuilder()
										.AddJsonFile(configFile, optional: false, reloadOnChange: false)
										.AddInMemoryCollection(cmdlineTaskParameters)
										.Build();
								var taskparms = taskconfig.Get<TaskParametersTemplate>().EnsureValid();

								// Create TaskParameters object from template object, and add to collection:
								adapterConfigs.Add(new TaskParameters(taskparms.Parameters)
								{
									TaskName = string.IsNullOrEmpty(taskparms.TaskName) ? Path.GetFileName(configFile) : taskparms.TaskName,
									AdapterClassName = taskparms.AdapterClassName,
									AdapterDLLName = taskparms.AdapterDLLName,
									AdapterDLLPath = taskparms.AdapterDLLPath,
									Configuration = taskconfig
								});
							}
							catch (Exception ex)
							{
								// Error reading file or deserializing configuration object; if this is not part of a batch OR
								// batch specifies halt on error, empty list and re-throw exception to stop all processing:
								if (string.IsNullOrEmpty(taskFolderName) || engineConfig.HaltOnError)
								{
									adapterConfigs.Clear();
									throw new Exception($"Error reading configuration file {configFile}", ex);
								}
								else // Otherwise just log error and proceed with other configurations:
								{
									Log.Logger.Warning(ex, $"Error reading configuration file {configFile}, skipped");
									logMessage.AppendLine($"Error reading configuration file {configFile}, skipped");
								}
							}
						}
					}
					#endregion
				}
				catch (Exception ex)
				{
					Log.Logger.Error(ex, "Error retrieving task configurations");
				}
				#endregion

				#region Execute TaskAdapter collection
				if (adapterConfigs.Count > 0)
				{
					// Activate TaskEngine service, and pass in TaskAdapter configuration collection for (synchronous) execution:
					using var te = serviceProvider.GetRequiredService<TaskEngine>();
					var result = te.Execute(adapterConfigs, engineConfig).Result;

					// Set executable exit code to execution return value, save log message for use below:
					Environment.ExitCode = result.ReturnValue;
					logMessage.Append(result.LogMessage);
				}
				else // No configurations available - exit with error message
				{
					Environment.ExitCode = 1;
					logMessage.Append("No task configurations available");
				}
				#endregion

				#region Notify users of execution results, if required
				if (logMessage.Length > 0)
				{
					//Console.Error.WriteLine(logMessage); // TODO: UNCOMMENT CONSOLE OUTPUT
				}

				// If configuration specifies that results should be emailed (or it specifies that errors should be
				// emailed and application is NOT exiting with success), send alert now:
				var sendresultsto = (string.IsNullOrEmpty(config["sendresult"]) && Environment.ExitCode != 0) ?
					config["senderror"] : config["sendresult"];
				if (!string.IsNullOrEmpty(sendresultsto))
				{
					using (var smtp = serviceProvider.GetService<TaskUtilities.Smtp>())
					{
						smtp.SendEmail($"Execution {(Environment.ExitCode == 0 ? "results" : "ERROR")} job [{jobName}] on {Environment.MachineName}",
							$"Return code {Environment.ExitCode} at {DateTime.Now:yyyy-MM-dd HH:mm:ss}<br/><br/><pre>{logMessage}</pre>",
							sendresultsto);
					}
				}
				#endregion
			}
		}
	}
}