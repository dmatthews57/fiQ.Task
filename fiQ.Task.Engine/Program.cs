using System;
using System.IO;
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
			var configbuilder = new ConfigurationBuilder()
				.SetBasePath(Directory.GetCurrentDirectory())
				.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
				.AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", optional: true, reloadOnChange: false)
				.AddEnvironmentVariables();
			if (args != null)
			{
				configbuilder.AddCommandLine(args);
			}
			var config = configbuilder.Build();

			// Build logger configuration from application config file and create logger:
			Log.Logger = new LoggerConfiguration()
				.ReadFrom.Configuration(config)
				.CreateLogger();

			// Set up services for dependency injection:
			var serviceCollection = new ServiceCollection()
				.AddLogging(configure => configure.AddSerilog())
				.AddSingleton<IConfiguration>(config)
			;

			using (var serviceProvider = serviceCollection.BuildServiceProvider())
			{
			}
		}
	}
}
