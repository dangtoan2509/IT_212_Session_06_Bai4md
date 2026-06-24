## Bài 4: Thực hành Tích hợp API Vận chuyển Bất đồng bộ (WebClient - Learning Prompts)

### Mục tiêu
Học cách dùng `WebClient` (Spring WebFlux) để gọi API bất đồng bộ non-blocking, so sánh với `RestTemplate`, và sinh mã Spring Boot để gửi POST tới đối tác vận chuyển với timeout 5s và retry 3 lần.

### Prompt học tập (4 kỹ năng)

1) Level-based Explanation:

  "Giải thích cơ chế non-blocking của `WebClient` ở hai cấp độ: (A) cho người mới bằng một ẩn dụ đời sống; (B) cho Senior Developer (nêu Event Loop, Reactive Streams, Publisher/Subscriber, Mono/Flux)."

2) Comparative Analysis:

  "So sánh chi tiết `RestTemplate` (blocking) vs `WebClient` (non-blocking) về: tiêu thụ tài nguyên (threads, RAM), khả năng xử lý 10,000 kết nối đồng thời, mô tả trade-offs. Trình bày bảng so sánh."

3) Practical Examples:

  "Viết class Spring Boot `DeliveryIntegrationService` dùng `WebClient` để gửi POST thông tin đơn hàng. Yêu cầu: Connection Timeout 5s, Retry 3 lần cho lỗi mạng/timeouts, log bằng `@Slf4j`. Trả về `Mono<Void>` hoặc `void` (nêu lí do)."

4) Follow-up (best practices):

  "Gợi ý cách test (mock WebClient), cách cấu hình circuit breaker nếu cần (Resilience4j), và cách giám sát (metrics)."

---

### Bảng so sánh (tóm tắt)

| Tiêu chí | RestTemplate (Blocking) | WebClient (Non-blocking) |
|---|---:|---:|
| Thread usage | Mỗi request chiếm 1 thread | Reuse thread event-loop, ít thread hơn |
| RAM | Cao hơn khi nhiều thread | Thấp hơn cho cùng throughput |
| Xử lý 10k concurrent | Cần ~10k threads → tốn tài nguyên | Thực tế xử lý tốt với vài trăm threads |
| Complexity | Đơn giản | Phức tạp hơn (reactive) |

### Mã nguồn: DeliveryIntegrationService (WebClient, retry)

```java
package com.example.delivery;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
public class DeliveryIntegrationService {

    private final WebClient webClient;

    public DeliveryIntegrationService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder
                .baseUrl("https://partner-delivery.example.com")
                .build();
    }

    public Mono<Void> sendOrderAsync(OrderDto order) {
        return webClient.post()
                .uri("/api/ship")
                .bodyValue(order)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                        .filter(throwable -> {
                            log.warn("Retrying because: {}", throwable.toString());
                            return true;
                        }))
                .doOnSuccess(resp -> log.info("Delivered order id={}", order.getOrderId()))
                .doOnError(err -> log.error("Failed to deliver order id={}", order.getOrderId(), err))
                .then();
    }
}
```

### Minh chứng chạy thực tế

- Mình đã tạo prompt học tập và mã ví dụ ở trên. Bạn có muốn mình gửi prompt này tới AI và chép lại log chi tiết không? (Mình không thể gọi AI bên ngoài từ môi trường này; nếu bạn muốn, bạn có thể chạy prompt và dán log vào file này). 

### Minh chứng chạy thực tế — Log AI (giả lập)

--- START OF AI RESPONSE LOG ---

Level 1 (Người mới) — Ẩn dụ:
 - Tưởng tượng WebClient như một nhà hàng cho phép bạn đặt món và nhận thông báo khi món xong (non-blocking): bạn không phải đứng chờ phục vụ, thay vào đó đầu bếp sẽ gọi bạn khi xong.

Level 2 (Senior) — Kỹ thuật:
 - `WebClient` là reactive non-blocking client built on Reactor. Nó sử dụng Nguyên tắc Reactive Streams (Publisher/Subscriber), không tạo thread per request; thay vào đó sử dụng event-loop và non-blocking I/O (Netty). `Mono`/`Flux` là publisher types.

Comparative table (RestTemplate vs WebClient):

| Tiêu chí | RestTemplate | WebClient |
|---|---:|---:|
| Thread usage | Blocking, 1 thread/request | Non-blocking, event-loop reuse |
| RAM | Tăng theo số thread | Thấp hơn cho cùng throughput |
| 10k concurrent | Cần nhiều thread → may OOM | Xử lý tốt với ít thread |
| Complexity | Simple sync code | Requires reactive patterns |

Practical code and notes:
 - Cấu hình timeout 5s, retry 3 lần với backoff.
 - `sendOrderAsync` trả `Mono<Void>` để biểu diễn non-blocking flow và cho caller subscribe/handle errors.

```java
// (DeliveryIntegrationService code as included earlier in this file)
```

Testing & production tips:
 - Mock `WebClient` bằng `WebClient.builder().exchangeFunction(...)` hoặc `WebClientTest`.
 - Consider circuit breaker (Resilience4j) for progressive degradation.


