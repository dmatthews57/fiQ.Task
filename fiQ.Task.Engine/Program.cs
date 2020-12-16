using System;
using System.IO;
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
			// Load application configuration:
			var env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
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
				configbuilder.AddCommandLine(args);
			}
			var config = configbuilder.Build();

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
			;
			using (var serviceProvider = serviceCollection.BuildServiceProvider())
			{

				// DO STUFF....

				using (var smtp = serviceProvider.GetService<SmtpUtilities>())
				{
					smtp.SendEmail("Test", "Test body", "dmatthews57@gmail.com");
				}
			}
		}
	}
}
