package com.paulsnow.qcdcf.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableIdTest {

    @Test
    void canonicalNameCombinesSchemaAndTable() {
        var tableId = new TableId("public", "customer");
        assertThat(tableId.canonicalName()).isEqualTo("public.customer");
    }

    @Test
    void toStringReturnsCanonicalName() {
        var tableId = new TableId("sales", "orders");
        assertThat(tableId.toString()).isEqualTo("sales.orders");
    }

    @Test
    void equalityBasedOnSchemaAndTable() {
        var a = new TableId("public", "customer");
        var b = new TableId("public", "customer");
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentTablesAreNotEqual() {
        var a = new TableId("public", "customer");
        var b = new TableId("public", "orders");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void nullSchemaThrows() {
        assertThatThrownBy(() -> new TableId(null, "customer"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullTableThrows() {
        assertThatThrownBy(() -> new TableId("public", null))
                .isInstanceOf(NullPointerException.class);
    }
}
