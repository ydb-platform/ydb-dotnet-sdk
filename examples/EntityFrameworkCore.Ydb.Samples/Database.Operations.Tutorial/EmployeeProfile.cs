namespace Database.Operations.Tutorial;

public class EmployeeProfile
{
    public int Id { get; set; }
    public required string Phone { get; set; }
    public required string Email { get; set; }

    public int EmployeeId { get; set; }
    public Employee Employee { get; set; } = null!;
}