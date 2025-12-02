using System.Diagnostics;
using Bogus;
using Elastic.Clients.Elasticsearch;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "My API", Version = "v1" });
});

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

var elasticUri = Environment.GetEnvironmentVariable("ELASTICSEARCH_HOSTS") ?? "http://localhost:9200";

var elasticClient = new ElasticsearchClient(new Uri(elasticUri));

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
    var result = await SearchPersons();
    
    return result.Count > 0 ? Results.Ok(result) : Results.NotFound();
});

app.MapDelete("/person/purge", async () => await Purge() ? Results.Ok() : Results.BadRequest());

app.Run();
return;

async Task<bool> IndexPersons(IEnumerable<Person> persons)
{
    try
    {
        var indexExists = await elasticClient.Indices.ExistsAsync("persons");

        if (!indexExists.Exists)
        {
            var index = await elasticClient.Indices.CreateAsync("persons");
            
            if (!index.IsValidResponse)
            {
                Debug.Print(index.DebugInformation);
                
                return false;
            }
        }
        
        var bulkInsert = await elasticClient.IndexManyAsync(persons, "persons");
        
        return bulkInsert.IsValidResponse;
    }
    catch (Exception ex)
    {
        Debug.Print($"[-] {ex.Message}");
        Debug.Print("--------------------------------");
        Debug.Print($"[-] {ex.InnerException?.Message}");
        
        return false;
    }
}

async Task<bool> Purge()
{
    try
    {
        var deletedIndex = await elasticClient.Indices.DeleteAsync("persons");
        
        return deletedIndex.IsValidResponse;
    }
    catch (Exception ex)
    {
        Debug.Print($"[-] {ex.Message}");
        Debug.Print("--------------------------------");
        Debug.Print($"[-] {ex.InnerException?.Message}");
        
        return false;
    }
}

async Task<List<Person>> SearchPersons()
{
    var response = await elasticClient.SearchAsync<Person>("persons");

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