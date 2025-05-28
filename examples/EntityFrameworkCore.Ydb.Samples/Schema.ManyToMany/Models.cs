namespace Schema.ManyToMany;

public class Department
{
    public int Id { get; set; }
    public required string Name { get; set; }

    // Collection navigation containing children
    public required ICollection<Employee> Employees { get; set; }
}

public class Employee
{
    public int Id { get; set; }
    public required string FirstName { get; set; }
    public required string LastName { get; set; }
    public required decimal Salary { get; set; }
    public required DateTime JoinedDate { get; set; }
    public int DepartmentId { get; set; }

    // Reference navigation to Department
    public Department Department { get; set; } = null!;

    // Reference navigation to EmployeeProfile
    public EmployeeProfile? Profile { get; set; }

    // collection navigation to Employee
    public List<Skill> Skills { get; set; } = new();
}

public class EmployeeProfile
{
    public int Id { get; set; }
    public required string Phone { get; set; }
    public required string Email { get; set; }

    // Required foreign key property
    public int EmployeeId { get; set; }

    // Required reference navigation to Employee
    public Employee Employee { get; set; } = null!;
}

public class Skill
{
    public int Id { get; set; }
    public required string Title { get; set; }

    // collection navigation to Employee
    public List<Employee> Employees { get; set; } = new();
}