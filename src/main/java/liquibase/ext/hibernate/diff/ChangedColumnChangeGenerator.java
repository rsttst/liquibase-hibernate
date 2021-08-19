package liquibase.ext.hibernate.diff;

import liquibase.change.Change;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.diff.Difference;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

import java.util.*;

/**
 * Hibernate and database types tend to look different even though they are not.
 * There are enough false positives that it works much better to suppress all column changes based on types.
 */
public class ChangedColumnChangeGenerator extends liquibase.diff.output.changelog.core.ChangedColumnChangeGenerator {

    private static final Set<String> IGNORED_DIFFERENCE_FIELDS;

    static {
        Set<String> ignoredDifferenceFields = new LinkedHashSet<>();
        ignoredDifferenceFields.add("order"); // This is just the ORDINAL_POSITION for plain columns which hibernate doesn't support
        IGNORED_DIFFERENCE_FIELDS = Collections.unmodifiableSet(ignoredDifferenceFields);
    }

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Column.class.isAssignableFrom(objectType)) {
            return PRIORITY_ADDITIONAL;
        }
        return PRIORITY_NONE;
    }

    @Override
    public Change[] fixChanged(DatabaseObject changedObject, ObjectDifferences differences, DiffOutputControl control,
                               Database referenceDatabase, Database comparisonDatabase, ChangeGeneratorChain chain) {
        boolean refDbIsHibernate;
        if (referenceDatabase instanceof HibernateDatabase && !(comparisonDatabase instanceof HibernateDatabase)) {
            refDbIsHibernate = true;
        } else if (!(referenceDatabase instanceof HibernateDatabase) && comparisonDatabase instanceof HibernateDatabase) {
            refDbIsHibernate = false;
        } else {
            return super.fixChanged(changedObject, differences, control, referenceDatabase, comparisonDatabase, chain);
        }

        IGNORED_DIFFERENCE_FIELDS.forEach(differences::removeDifference);

        // Check if any present type reference is actually a difference or just an alias
        Difference typeDifference = differences.getDifference("type");
        if (typeDifference != null) {
            Database nonHibernateDatabase = refDbIsHibernate ? comparisonDatabase : referenceDatabase;
            LiquibaseDataType referenceDatatype = DataTypeFactory.getInstance().from(
                    (DataType) typeDifference.getReferenceValue(), nonHibernateDatabase
            );
            LiquibaseDataType comparisonDatatype = DataTypeFactory.getInstance().from(
                    (DataType) typeDifference.getComparedValue(), nonHibernateDatabase
            );
            if (referenceDatatype.equals(comparisonDatatype)) {
                differences.removeDifference("type");
            }
        }

        // non-hibernate databases sometimes adds a function default value, like for timestamp columns
        Difference defaultValueDifference = differences.getDifference("defaultValue");
        if (defaultValueDifference != null) {
            if (refDbIsHibernate) {
                if (defaultValueDifference.getReferenceValue() == null
                        && defaultValueDifference.getComparedValue() instanceof DatabaseFunction) {
                    differences.removeDifference("defaultValue");
                }
            } else {
                if (defaultValueDifference.getComparedValue() == null
                        && defaultValueDifference.getReferenceValue() instanceof DatabaseFunction) {
                    differences.removeDifference("defaultValue");
                }
            }
        }

        return super.fixChanged(changedObject, differences, control, referenceDatabase, comparisonDatabase, chain);
    }

}
