using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using fiQ.Task.Models;
using fiQ.Task.Utilities;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

namespace fiQ.Task.Engine
{
	class Program
	{
		static void Main(string[] args)
		{
			string jobName = null; // For logging purposes
			string taskFileName = null; // For running a single file
			string taskDirName = null; // For running all files in a directory
			bool haltOnError = false; // Flag to stop running directory if error encountered
			var overrideTaskParameters = new TaskParameters(); // Global overrides for task parameters

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
						overrideTaskParameters.AddParameter(match.Groups["name"].Value, match.Groups["value"].Value);
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
									haltOnError = true;
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
									case "taskdir": // Name of directory of task files to execute
										taskDirName = match.Groups["value"].Value;
										break;
									default: // Unrecognized parameter - add to config collection
										configargs.Add(arg);
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
				.CreateLogger();

			// Set up services and configurations for dependency injection:
			var serviceCollection = new ServiceCollection()
				.AddLogging(configure => configure.AddSerilog())
				.AddSingleton<IConfiguration>(config)
				.Configure<SmtpOptions>(config.GetSection("Smtp"))
				.AddTransient<SmtpUtilities>()
				.AddSingleton<TaskService>()
			;
			using (var serviceProvider = serviceCollection.BuildServiceProvider())
			{
				serviceProvider.GetRequiredService<TaskService>().DoSomething(jobName, taskDirName, taskFileName, haltOnError, overrideTaskParameters);

				// DO STUFF....
				/*
				using (var smtp = serviceProvider.GetService<SmtpUtilities>())
				{
					smtp.SendEmail("Test", "Test body", "dmatthews57@gmail.com");
				}
				*/
			}
		}
	}
}
