namespace Database.Operations.Tutorial;

public class Skill
{
    public int Id { get; set; }
    public required string Title { get; set; }

    // collection navigation to Employee
    public List<Employee> Employees { get; set; } = new();
}