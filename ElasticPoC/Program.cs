using Bogus;
using Elastic.Clients.Elasticsearch;
using Elastic.Serilog.Sinks;
using Microsoft.OpenApi.Models;
using Serilog;
using DataStreamName = Elastic.Ingest.Elasticsearch.DataStreams.DataStreamName;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "My API", Version = "v1" });
});

var elasticUri = !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("ELASTICSEARCH_HOSTS")) ? 
    new Uri(Environment.GetEnvironmentVariable("ELASTICSEARCH_HOSTS")) : 
    new Uri("http://localhost:9200");

var elasticClient = new ElasticsearchClient(elasticUri);

Log.Logger = new LoggerConfiguration()
    .WriteTo.Elasticsearch([elasticUri], opts =>
    {
        opts.DataStream = new DataStreamName("logs", "test");
    })
    .Enrich.FromLogContext()
    .CreateLogger();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

app.MapPost("/person", async () =>
{
    var personFaker = new Faker<Person>()
        .RuleFor(p => p.Id, f => f.Random.Guid())
        .RuleFor(p => p.FirstName, f => f.Person.FirstName)
        .RuleFor(p => p.LastName, f => f.Person.LastName)
        .RuleFor(p => p.Email, f => f.Person.Email);
    
    var persons = personFaker.Generate(5);
    
    return await IndexPersons(persons) ? Results.Created() : Results.BadRequest();
});

app.MapGet("/person", async () =>
{
    var result = await GetAllPersons();
    
    return result.Count > 0 ? Results.Ok(result) : Results.NotFound();
});

app.MapDelete("/person/purge", async () => await Purge() ? Results.Ok() : Results.BadRequest());

app.MapGet("/person/search", async (string name) =>
{
    var result = await GetAllPersons(name);
    
    return result.Count > 0 ? Results.Ok(result) : Results.NotFound();
});

app.Run();
return;

async Task<bool> IndexPersons(IEnumerable<Person> persons)
{
    try
    {
        var indexExists = await elasticClient.Indices.ExistsAsync("persons");

        if (!indexExists.Exists)
        {
            Log.Information("[*] Creating 'persons' index...");
            
            var index = await elasticClient.Indices.CreateAsync("persons");
            
            if (!index.IsValidResponse)
            {
                Log.Information($"[*] {index.DebugInformation}");
                
                return false;
            }
        }
        
        var bulkInsert = await elasticClient.IndexManyAsync(persons, "persons");
        
        return bulkInsert.IsValidResponse;
    }
    catch (Exception ex)
    {
        Log.Information($"[*] {ex.Message}");
        Log.Information($"[*] {ex.InnerException?.Message}");
        
        return false;
    }
}

async Task<bool> Purge()
{
    try
    {
        Log.Information("[*] Deleting 'persons' index...");
        
        var deletedIndex = await elasticClient.Indices.DeleteAsync("persons");
        
        return deletedIndex.IsValidResponse;
    }
    catch (Exception ex)
    {
        Log.Information($"[*] {ex.Message}");
        Log.Information($"[*] {ex.InnerException?.Message}");
        
        return false;
    }
}

async Task<List<Person>> GetAllPersons(string? name = null)
{
    Log.Information("[*] Searching for persons...");
    
    var response = await elasticClient.SearchAsync<Person>("persons", s => s.Query(q =>
    {
        if (string.IsNullOrWhiteSpace(name))
            q.MatchAll();
        else
            q.Match(m => m.Field(f => f.FirstName).Fuzziness(new Fuzziness("Auto")).Query(name));
    })); 
    return response.IsValidResponse ? response.Documents.ToList() : [];
}

public class Person
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    
    public Person()
    {
        
    }
    
    public Person(Guid id, string firstName, string lastName, string email)
    {
        Id = id;
        FirstName = firstName;
        LastName = lastName;
        Email = email;
    }
}