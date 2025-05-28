using Database.Operations.Tutorial;
using Microsoft.EntityFrameworkCore;

var orderingActions = new List<Action>
{
    Action.ReadCsv,
    Action.Select,
    Action.OrderBy,
    Action.QueryLinq,
    Action.Where,
    Action.InOperator,
    Action.Like,
    Action.InnerJoin,
    Action.GroupBy
};
var actions = new Dictionary<Action, Action<HRContext>>
{
    { Action.ReadCsv, ReadCSV },
    { Action.Select, Select },
    { Action.OrderBy, OrderBy },
    { Action.QueryLinq, QueryLinq },
    { Action.Where, Where },
    { Action.InOperator, InOperator },
    { Action.Like, Like },
    { Action.InnerJoin, InnerJoin },
    { Action.GroupBy, GroupBy }
};

switch (args.Length)
{
    case > 0:
    {
        if (Enum.TryParse<Action>(args[0], true, out var result))
        {
            using var hrContext = new HRContext();
            actions[result](hrContext);
            break;
        }

        throw new InvalidOperationException("Not found argument: " + args[0]);
    }
    case 0:
    {
        foreach (var action in orderingActions)
        {
            using var context = new HRContext();
            actions[action](context);
        }

        break;
    }
}

return;

static void GroupBy(HRContext context)
{
    {
        var groups = context.Employees
            .GroupBy(e => e.Department)
            .Select(group => new
            {
                group.Key.Name,
                Headcount = group.Count()
            })
            .OrderBy(dc => dc.Name)
            .ToList();

        foreach (var group in groups)
        {
            Console.WriteLine($"{group.Name,-20}{group.Headcount}");
        }
    }
    {
        var groups = context.Employees
            .GroupBy(e => e.Department)
            .Select(group => new
            {
                group.Key.Name,
                Headcount = group.Count()
            })
            .Where(group => group.Headcount > 11)
            .OrderBy(dc => dc.Name)
            .ToList();

        foreach (var group in groups)
        {
            Console.WriteLine($"{group.Name,-20}{group.Headcount}");
        }
    }
}

static void InnerJoin(HRContext context)
{
    {
        Console.WriteLine("Get employees & departments");

        var employees = context.Employees.Include(e => e.Department)
            .OrderBy(e => e.FirstName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName} - {e.Department.Name}");
        }
    }
    {
        Console.WriteLine("EF Core Inner Join in a many-to-many relationship");
        var employees = context.Employees.Include(e => e.Skills)
            .OrderBy(e => e.FirstName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");

            foreach (var skill in e.Skills)
            {
                Console.WriteLine($"- {skill.Title}");
            }
        }
    }
    {
        Console.WriteLine("Using multiple joins");
        var employees = context.Employees.Include(e => e.Department)
            .Include(e => e.Skills)
            .OrderBy(e => e.FirstName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName} - {e.Department.Name}");

            foreach (var skill in e.Skills)
            {
                Console.WriteLine($"- {skill.Title}");
            }
        }
    }
}

static void Like(HRContext context)
{
    {
        Console.WriteLine("Using the % wildcard character");

        var keyword = "%ac%";
        var employees = context.Employees
            .Where(e => EF.Functions.Like(e.FirstName, keyword))
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
    {
        var keyword = "da%";
        var employees = context.Employees
            .Where(e => EF.Functions.Like(e.FirstName, keyword))
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
    {
        Console.WriteLine("Using the _ wildcard character");

        var keyword = "H_n%";
        var employees = context.Employees
            .Where(e => EF.Functions.Like(e.FirstName, keyword))
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
}

static void InOperator(HRContext context)
{
    {
        Console.WriteLine("Introduction to EF Core Where In");

        int[] ids = [1, 2, 3];

        var employees = context.Employees
            .Where(e => ids.Contains(e.Id))
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.Id} - {e.FirstName} {e.LastName}");
        }
    }

    {
        Console.WriteLine("NOT IN");

        int[] ids = [1, 2, 3];

        var employees = context.Employees
            .Where(e => !ids.Contains(e.Id))
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.Id} - {e.FirstName} {e.LastName}");
        }
    }
}

static void Where(HRContext context)
{
    {
        Console.WriteLine("Using the Where() method with the equal operator");
        // ReSharper disable once RedundantAssignment
        var employees = context.Employees
            .Where(e => e.FirstName == "Alexander")
            .ToList();

        // ReSharper disable once ConvertToConstant.Local
        var firstName = "Alexander";
        employees = context.Employees
            .Where(e => e.FirstName == firstName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
    {
        Console.WriteLine("Using AND operator");
        var firstName = "Alexander";
        var lastName = "Young";

        var employees = context.Employees
            .Where(e => e.FirstName == firstName && e.LastName == lastName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
    {
        var startDate = new DateTime(2023, 3, 1);
        var endDate = new DateTime(2023, 3, 31);

        var employees = context.Employees
            .Where(e => e.JoinedDate >= startDate && e.JoinedDate <= endDate)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName} - {e.JoinedDate.ToShortDateString()}");
        }
    }
    {
        Console.WriteLine("Using OR operator");
        var firstName = "Emily";
        var lastName = "Brown";

        var employees = context.Employees
            .Where(e => e.FirstName == firstName || e.LastName == lastName)
            .ToList();

        foreach (var e in employees)
        {
            Console.WriteLine($"{e.FirstName} {e.LastName}");
        }
    }
}

static void QueryLinq(HRContext context)
{
    {
        Console.WriteLine("EF Core Query flow");

        var departments = (from d in context.Departments select d).ToList();

        foreach (var department in departments)
        {
            Console.WriteLine($"{department.Id}: {department.Name}");
        }
    }
    {
        Console.WriteLine("Query enumeration");

        foreach (var d in context.Departments)
        {
            Console.WriteLine($"{d.Id} {d.Name}");
        }
    }
    {
        Console.WriteLine("Filtering data (WHERE Name == 'Sales')");
        var name = "Sales";
        // ReSharper disable once RedundantAssignment
        var department = context.Departments
            .Where(d => d.Name == "Sales")
            .ToList();

        department = context.Departments
            .Where(d => d.Name == name)
            .ToList();

        foreach (var d in department)
        {
            Console.WriteLine($"{d.Id} {d.Name}");
        }
    }
    {
        Console.WriteLine("Filtering partial texts (LIKE)");
        var keyword = "i";
        var departments = context.Departments
            .Where(d => EF.Functions.Like(d.Name, $"%{keyword}%"))
            .ToList();

        foreach (var d in departments)
        {
            Console.WriteLine($"{d.Id} {d.Name}");
        }
    }
    {
        Console.WriteLine("Finding an entity by its key value");
        var department = context.Departments.Find(1);

        Console.WriteLine($"{department?.Id} {department?.Name}");
    }
}

static void OrderBy(HRContext context)
{
    Console.WriteLine("Sorting by one column in ascending order");

    var listByOneColumnInAscendingOrder = context.Employees
        .OrderBy(e => e.FirstName)
        .Take(7)
        .ToList();

    foreach (var e in listByOneColumnInAscendingOrder)
    {
        Console.WriteLine(e.FirstName);
    }

    Console.WriteLine("Sorting by two or more columns in ascending order");

    var listByTwoOrMoreColumnsInAscendingOrder = context.Employees
        .OrderBy(e => e.FirstName)
        .ThenBy(e => e.LastName)
        .Take(7)
        .ToList();

    foreach (var e in listByTwoOrMoreColumnsInAscendingOrder)
    {
        Console.WriteLine($"{e.FirstName} {e.LastName}");
    }

    Console.WriteLine("Sorting by one column in descending order");

    var listByOneColumnInDescendingOrder = context.Employees
        .OrderByDescending(e => e.FirstName)
        .Take(7)
        .ToList();

    foreach (var e in listByOneColumnInDescendingOrder)
    {
        Console.WriteLine(e.FirstName);
    }

    Console.WriteLine("Sorting by two or more columns in descending order");

    var listByTwoOrMoreColumnsInDescendingOrder = context.Employees
        .OrderByDescending(e => e.FirstName)
        .ThenByDescending(e => e.LastName)
        .Take(7)
        .ToList();

    foreach (var e in listByTwoOrMoreColumnsInDescendingOrder)
    {
        Console.WriteLine(e.FirstName);
    }

    Console.WriteLine("Sorting one column in ascending order and another column in descending order");

    var list = context.Employees
        .OrderBy(e => e.JoinedDate)
        .ThenByDescending(e => e.FirstName)
        .Take(6)
        .ToList();

    foreach (var e in list)
    {
        Console.WriteLine($"{e.JoinedDate.ToShortDateString()} {e.LastName}");
    }
}

static void Select(HRContext context)
{
    Console.WriteLine("Selecting all rows from a table");

    var employees = context.Employees.ToList();

    foreach (var e in employees)
    {
        Console.WriteLine($"{e.FirstName} {e.LastName}");
    }

    Console.WriteLine("Selecting some columns from a table");

    var names = context.Employees
        .Select(e => $"{e.FirstName} {e.LastName}")
        .ToList();

    foreach (var name in names)
    {
        Console.WriteLine(name);
    }

    Console.WriteLine("Returning a list of anonymous objects");

    var list = context.Employees
        .Select(e => new { e.FirstName, e.JoinedDate })
        .ToList();

    foreach (var e in list)
    {
        Console.WriteLine($"{e.FirstName} - {e.JoinedDate.ToShortDateString()}");
    }
}

static void ReadCSV(HRContext context)
{
    var employees = new List<Employee>();
    var skills = new List<Skill>();
    var departments = new List<Department>();

    using var reader = new StreamReader("data.csv");

    // Read the header line
    _ = reader.ReadLine();

    while (!reader.EndOfStream)
    {
        var line = reader.ReadLine();
        if (line == null)
        {
            break;
        }

        // Split the line by comma
        var values = line.Split(',');

        if (values.Length == 8)
        {
            var firstName = values[0];
            var lastName = values[1];
            var salary = values[2];
            var joinedDate = DateTime.ParseExact(values[3], "M/d/yyyy", null);
            var phone = values[4];
            var email = values[5];
            var departmentName = values[6];
            var skillTitles = values[7].Split(';');

            // Create Department object if it doesn't exist
            var department = departments.Find(d => d.Name == departmentName);
            if (department == null)
            {
                department = new Department
                {
                    Name = departmentName
                };
                departments.Add(department);
            }

            // Create EmployeeProfile object
            var profile = new EmployeeProfile
            {
                Phone = phone,
                Email = email
            };

            // Create Employee object
            var employee = new Employee
            {
                FirstName = firstName,
                LastName = lastName,
                Salary = decimal.Parse(salary),
                JoinedDate = joinedDate,
                Department = department,
                Profile = profile
            };
            employees.Add(employee);

            // Create Skill objects
            foreach (var skillTitle in skillTitles)
            {
                var skill = skills.Find(s => s.Title == skillTitle);
                if (skill == null)
                {
                    skill = new Skill
                    {
                        Title = skillTitle
                    };
                    skills.Add(skill);
                }

                // Add skill to the employee's Skills collection
                employee.Skills.Add(skill);
            }
        }
    }

    Console.WriteLine($"{employees.Count} row(s) found");
    foreach (var employee in employees)
    {
        context.Add(employee);
    }

    context.SaveChanges();
}

internal enum Action
{
    ReadCsv,
    Select,
    OrderBy,
    QueryLinq,
    Where,
    InOperator,
    Like,
    InnerJoin,
    GroupBy
}