package com.michelin.ns4kafka.models;

import com.michelin.ns4kafka.validation.FieldValidationException;
import com.michelin.ns4kafka.validation.ResourceValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ResourceValidatorTest {
    @Test
    void testNonEmptyString() {
        ResourceValidator.Validator original = new ResourceValidator.NonEmptyString();
        ResourceValidator.Validator same = new ResourceValidator.NonEmptyString();
        // Test Equals
        Assertions.assertEquals(original, same);

        // test ensureValid
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        //Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", " "));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "v"));
    }

    @Test
    void testRangeBetween() {
        // BETWEEN
        ResourceValidator.Validator original = ResourceValidator.Range.between(0, 10);
        ResourceValidator.Validator same = ResourceValidator.Range.between(0, 10);
        ResourceValidator.Validator different = ResourceValidator.Range.between(0, 99);
        // Test Equals
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, different);
        // test ensureValid
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "11"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "0"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "5"));

    }
    @Test
    void testOptionalRange() {
        // BETWEEN
        ResourceValidator.Validator original = new ResourceValidator.Range(0,10,true);
        // test ensureValid

        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "11"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", null));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "0"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "5"));

    }
    @Test
    void testRangeAtLeast() {
        ResourceValidator.Validator original = ResourceValidator.Range.atLeast(10);
        ResourceValidator.Validator same = ResourceValidator.Range.atLeast(10);
        ResourceValidator.Validator different = ResourceValidator.Range.atLeast(99);
        // Test Equals
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, different);
        // test ensureValid
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "NotANumber"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "-1"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "9"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "10"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "11"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "1622760340000"));
    }

    @Test
    void testValidString() {
        ResourceValidator.Validator original = ResourceValidator.ValidString.in("a", "b", "c");
        ResourceValidator.Validator same = ResourceValidator.ValidString.in("a", "b", "c");
        ResourceValidator.Validator different = ResourceValidator.ValidString.in("b", "c", "d");
        ResourceValidator.Validator invertedDifferent = ResourceValidator.ValidString.in("c", "b", "a");
        // Test Equals
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, different);
        Assertions.assertNotEquals(original, invertedDifferent);
        // test ensureValid
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "c"));
    }

    @Test
    void testValidList() {
        ResourceValidator.Validator original = ResourceValidator.ValidList.in("a", "b", "c");
        ResourceValidator.Validator same = ResourceValidator.ValidList.in("a", "b", "c");
        ResourceValidator.Validator different = ResourceValidator.ValidList.in("b", "c", "d");
        ResourceValidator.Validator invertedDifferent = ResourceValidator.ValidList.in("c", "b", "a");
        // Test Equals
        Assertions.assertEquals(original, same);
        Assertions.assertNotEquals(original, different);
        Assertions.assertNotEquals(original, invertedDifferent);
        // test ensureValid
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", null));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", ""));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "A"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "d"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "1"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,A"));
        Assertions.assertThrows(FieldValidationException.class, () -> original.ensureValid("k", "a,b,c,d"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "a"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "b"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "c"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "a,b"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "b,c"));
        Assertions.assertDoesNotThrow(() -> original.ensureValid("k", "c,b,a"));
    }
}
