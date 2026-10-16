using System.ComponentModel.DataAnnotations;

namespace RabbitMqPractice.Api.Models.Requests
{
    public record PurchaseRequest
    {
        [Range(1, 100)]
        public required int Quantity { get; init; }

        [Required]
        [StringLength(maximumLength: 100)]
        public required string ProductName { get; init; }
    }
}
