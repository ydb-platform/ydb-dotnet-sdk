// See https://aka.ms/new-console-template for more information

using Section_1.HR;

InsertDepartments();
SelectDepartments();

return;


static void InsertDepartments()
{
    var departments = new List<Department>
    {
        new() { Name = "Sales" },
        new() { Name = "Marketing" },
        new() { Name = "Logistics" },
    };

    using var context = new HRContext();

    foreach (var department in departments)
    {
        context.Departments.Add(department);
    }

    context.SaveChanges();
}

static void SelectDepartments()
{
    using var context = new HRContext();
    var departments = context.Departments.ToList();

    foreach (var department in departments)
    {
        Console.WriteLine($"{department.Id} - {department.Name}");
    }
}