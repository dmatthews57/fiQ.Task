using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace fiQ.Task.Models
{
	[DataContract]
	class TaskAdapterConfig
	{
		#region Fields
		[DataMember]
		public string AdapterClassName { get; set; }
		[DataMember]
		public string AdapterDLLName { get; set; } = null;
		[DataMember]
		public string AdapterDLLPath { get; set; } = null;
		[DataMember]
		[JsonConverter(typeof(TaskParametersJsonConverter))]
		public Dictionary<string, string> TaskParameters { get; set; }
		#endregion

		#region Properties
		[IgnoreDataMember]
		public bool Valid
		{
			get
			{
				return (!string.IsNullOrEmpty(AdapterClassName) && TaskParameters?.Count > 0);
			}
		}
		#endregion

		#region JsonConverter classes
		public class TaskParametersJsonConverter : JsonConverter<Dictionary<string, string>>
		{
			public override Dictionary<string, string> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
			{
				if (reader.TokenType != JsonTokenType.StartObject)
				{
					throw new JsonException($"Invalid reader token type {reader.TokenType}");
				}

				// Create dictionary of the type we want (case-insensitive keys), and Regex to validate key names:
				var dictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
				var REGEX_KEYNAME = new Regex(@"^@\S+");

				// Read all available tokens until we reach end:
				while (reader.Read() ? (reader.TokenType != JsonTokenType.EndObject) : false)
				{
					// Read key name from reader; unless key is in format we expect (i.e. starts with "@"), ignore:
					var propertyName = reader.GetString();
					reader.Read();
					if (REGEX_KEYNAME.IsMatch(propertyName))
					{
						dictionary[propertyName[1..]] = reader.GetString();
					}
				}
				return dictionary;
			}

			/// <summary>
			/// Perform default write operation (no custom logic needed)
			/// </summary>
			public override void Write(Utf8JsonWriter writer, Dictionary<string, string> value, JsonSerializerOptions options)
			{
				JsonSerializer.Serialize(writer, value, options);
			}
		}
		#endregion
	}
}
