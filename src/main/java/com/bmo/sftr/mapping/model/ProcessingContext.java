package com.bmo.sftr.mapping.model;

import tools.jackson.databind.JsonNode;

public record ProcessingContext(JsonNode refData, SftrConstants constants, String fileDate) {}
