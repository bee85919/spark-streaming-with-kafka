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
public class ClickEvent {
    private String impId;
    private String clickUrl;
    private long timestamp;
}