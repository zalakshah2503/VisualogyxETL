using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Text;

namespace VisualogyxETL
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            var OriginDatabaseContextConnection = @"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=OriginDatabase;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
            var ReplicaDatabaseContextConnection = @"Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=ReplicaDatabase;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=True;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
            services.AddDbContext<OriginDatabaseContext>(options =>
                    options.UseSqlServer(OriginDatabaseContextConnection))
                    .AddDbContext<ReplicaDatabaseContext>(options =>
                    options.UseSqlServer(ReplicaDatabaseContextConnection));
        }
    }
}
