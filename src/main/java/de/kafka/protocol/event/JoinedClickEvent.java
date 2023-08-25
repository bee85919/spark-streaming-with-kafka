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
public class JoinedClickEvent {
    private String impId;
    private String requestId;
    private String adId;
    private String userId;
    private String deviceId;
    private String inventoryId;
    private long impTimestamp;
    private String clickUrl;
    private long timestamp;

    public static JoinedClickEvent from(ImpressionEvent impressionEvent, ClickEvent clickEvent) {
        return new JoinedClickEvent(
                impressionEvent.getImpId(),
                impressionEvent.getRequestId(),
                impressionEvent.getAdId(),
                impressionEvent.getUserId(),
                impressionEvent.getDeviceId(),
                impressionEvent.getInventoryId(),
                impressionEvent.getTimestamp(),
                clickEvent.getClickUrl(),
                clickEvent.getTimestamp()
        );
    }
}