package com.michelin.ns4kafka.models;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.michelin.ns4kafka.validation.FieldValidationException;
import com.michelin.ns4kafka.validation.ResourceValidator;
import org.junit.jupiter.api.Test;

class ResourceValidatorTest {
    @Test
    void testNonEmptyString() {
        ResourceValidator.Validator original = new ResourceValidator.NonEmptyString();
        ResourceValidator.Validator same = new ResourceValidator.NonEmptyString();
        // Test Equals
        assertEquals(original, same);

        // test ensureValid
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        //assertThrows(FieldValidationException.class, () -> original.ensureValid("k", " "));
        assertDoesNotThrow(() -> original.ensureValid("k", "v"));
    }

    @Test
    void testRangeBetween() {
        // BETWEEN
        ResourceValidator.Validator original = ResourceValidator.Range.between(0, 10);
        ResourceValidator.Validator same = ResourceValidator.Range.between(0, 10);
        ResourceValidator.Validator different = ResourceValidator.Range.between(0, 99);
        // Test Equals
        assertEquals(original, same);
        assertNotEquals(original, different);
        // test ensureValid
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "11"));
        assertDoesNotThrow(() -> original.ensureValid("k", "0"));
        assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        assertDoesNotThrow(() -> original.ensureValid("k", "5"));
    }

    @Test
    void testOptionalRange() {
        // BETWEEN
        ResourceValidator.Validator original = new ResourceValidator.Range(0, 10, true);
        // test ensureValid

        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "11"));
        assertDoesNotThrow(() -> original.ensureValid("k", null));
        assertDoesNotThrow(() -> original.ensureValid("k", "0"));
        assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        assertDoesNotThrow(() -> original.ensureValid("k", "5"));

    }

    @Test
    void testRangeAtLeast() {
        ResourceValidator.Validator original = ResourceValidator.Range.atLeast(10);
        ResourceValidator.Validator same = ResourceValidator.Range.atLeast(10);
        ResourceValidator.Validator different = ResourceValidator.Range.atLeast(99);
        // Test Equals
        assertEquals(original, same);
        assertNotEquals(original, different);
        // test ensureValid
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "9"));
        assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        assertDoesNotThrow(() -> original.ensureValid("k", "11"));
        assertDoesNotThrow(() -> original.ensureValid("k", "1622760340000"));
    }

    @Test
    void testValidString() {
        ResourceValidator.Validator original = ResourceValidator.ValidString.in("a", "b", "c");
        ResourceValidator.Validator same = ResourceValidator.ValidString.in("a", "b", "c");
        ResourceValidator.Validator different = ResourceValidator.ValidString.in("b", "c", "d");
        ResourceValidator.Validator invertedDifferent = ResourceValidator.ValidString.in("c", "b", "a");
        // Test Equals
        assertEquals(original, same);
        assertNotEquals(original, different);
        assertNotEquals(original, invertedDifferent);
        // test ensureValid
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c"));
    }

    @Test
    void testOptionalValidString() {
        ResourceValidator.Validator original = ResourceValidator.ValidString.optionalIn("a", "b", "c");

        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        assertDoesNotThrow(() -> original.ensureValid("k", null));
        assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c"));
    }

    @Test
    void testValidList() {
        ResourceValidator.Validator original = ResourceValidator.ValidList.in("a", "b", "c");
        ResourceValidator.Validator same = ResourceValidator.ValidList.in("a", "b", "c");
        ResourceValidator.Validator different = ResourceValidator.ValidList.in("b", "c", "d");
        ResourceValidator.Validator invertedDifferent = ResourceValidator.ValidList.in("c", "b", "a");
        // Test Equals
        assertEquals(original, same);
        assertNotEquals(original, different);
        assertNotEquals(original, invertedDifferent);
        // test ensureValid
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,b,c,d"));
        assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c"));
        assertDoesNotThrow(() -> original.ensureValid("k", "a,b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b,c"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c,b,a"));
    }

    @Test
    void testOptionalValidList() {
        ResourceValidator.Validator original = ResourceValidator.ValidList.optionalIn("a", "b", "c");
        // test ensureValid

        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,A"));
        assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,b,c,d"));
        assertDoesNotThrow(() -> original.ensureValid("k", null));
        assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c"));
        assertDoesNotThrow(() -> original.ensureValid("k", "a,b"));
        assertDoesNotThrow(() -> original.ensureValid("k", "b,c"));
        assertDoesNotThrow(() -> original.ensureValid("k", "c,b,a"));
    }
}
