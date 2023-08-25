package de.kafka.protocol.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class ImpressionEvent {
    private String impId;
    private String requestId;
    private String adId;
    private String userId;
    private String deviceId;
    private String inventoryId;
    private long timestamp;
}