namespace Schema.OneToMany;

public class Department
{
    public int Id { get; set; }
    public required string Name { get; set; }

    // Collection navigation containing children
    public ICollection<Employee> Employees { get; set; }
}

public class Employee
{
    public int Id { get; set; }
    public required string FirstName { get; set; }
    public required string LastName { get; set; }
    public required decimal Salary { get; set; }
    public required DateTime JoinedDate { get; set; }

    // Required foreign key property    
    public int DepartmentId { get; set; }

    //  Required reference navigation to parent 
    public Department Department { get; set; } = null!;
}