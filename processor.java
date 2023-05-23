import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncomeFlexService {

    private RestTemplate restTemplate;

    public IncomeFlexService() {
        this.restTemplate = new RestTemplate();
    }

    public double getIncomeFlexBalance(String indId) {
        String authApiUrl = "https://api.example.com/auth"; // API endpoint to generate auth token
        String incomeFlexApiUrl = "https://api.example.com/incomeflex/{indId}/balance"; // IncomeFlex API endpoint

        // Make the initial call to the auth API to get the auth token
        HttpHeaders authHeaders = new HttpHeaders();
        // Set any required headers for the auth API call
        // ...

        HttpEntity<?> authRequestEntity = new HttpEntity<>(authHeaders);
        ResponseEntity<Void> authResponseEntity = restTemplate.exchange(
                authApiUrl,
                HttpMethod.GET,
                authRequestEntity,
                Void.class
        );

        // Extract the auth token from the response headers
        HttpHeaders authResponseHeaders = authResponseEntity.getHeaders();
        String authToken = authResponseHeaders.getFirst(HttpHeaders.AUTHORIZATION);

        // Use the auth token in the subsequent request to the IncomeFlex API
        HttpHeaders incomeFlexHeaders = new HttpHeaders();
        incomeFlexHeaders.set(HttpHeaders.AUTHORIZATION, authToken);
        // Set any other required headers for the IncomeFlex API call
        // ...

        HttpEntity<?> incomeFlexRequestEntity = new HttpEntity<>(incomeFlexHeaders);
        ResponseEntity<IncomeFlexResponse> incomeFlexResponseEntity = restTemplate.exchange(
                incomeFlexApiUrl,
                HttpMethod.GET,
                incomeFlexRequestEntity,
                IncomeFlexResponse.class,
                indId
        );

        if (incomeFlexResponseEntity.getStatusCode().is2xxSuccessful()) {
            IncomeFlexResponse responseBody = incomeFlexResponseEntity.getBody();
            if (responseBody != null) {
                return responseBody.getBalance();
            }
        }

        // Return a default value or handle error cases
        return 0.0;
    }
}
