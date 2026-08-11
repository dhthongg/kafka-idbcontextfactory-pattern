using Microsoft.EntityFrameworkCore;

namespace OrderEvents.Consumer.Persistence;

public class OrdersDbContext : DbContext
{
    public OrdersDbContext(DbContextOptions<OrdersDbContext> options) : base(options) { }

    public DbSet<OrderRecord> OrderRecords => Set<OrderRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderRecord>(builder =>
        {
            builder.ToTable("OrderRecords");
            builder.HasKey(o => o.OrderId);
            builder.Property(o => o.Currency).HasMaxLength(3);
            builder.Property(o => o.TotalAmount).HasColumnType("decimal(18,2)");
        });
    }
}
