namespace Section_3.ProjectEF;

public class Employee
{
    public int Id { get; set; }
    public required string FirstName {  get; set; } 
    public required string LastName {  get; set; }
    public required decimal Salary { get; set; }
    public required DateTime JoinedDate { get; set; }
    
    // Foreign key property to the Department
    public int DepartmentId  {   get; set; }

    // Reference navigation to Department
    public Department Department { get; set; } = null!;

    // Reference navigation to EmployeeProfile
    public EmployeeProfile? Profile  { get; set; }

    // collection navigation to Employee
    public List<Skill> Skills { get; set; } = new();
}
