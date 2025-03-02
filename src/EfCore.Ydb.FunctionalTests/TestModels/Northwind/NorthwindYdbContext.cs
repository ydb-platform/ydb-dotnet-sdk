using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

namespace EfCore.Ydb.FunctionalTests.TestModels.Northwind;

public class NorthwindYdbContext(DbContextOptions options) : NorthwindRelationalContext(options);