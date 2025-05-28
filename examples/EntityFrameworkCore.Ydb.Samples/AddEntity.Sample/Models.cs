namespace Section_1.HR;

public class Department
{
    public int Id { get; set; }
    public string Name { get; set; }
}

public class Employee
{
    public int Id { get; set; }
    public required string FirstName { get; set; }
    public required string LastName { get; set; }
    public required DateTime JoinedDate { get; set; }
    public required decimal Salary { get; set; }
    public Department? Department { get; set; }
}