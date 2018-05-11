/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.memgroupby;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.injection.AfterInjection;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaNone;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.groupby.BaseGroupByMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Created on 02-jun-2003
 *
 */

@InjectionSupported( localizationPrefix = "MemoryGroupBy.Injection.", groups = { "FIELDS", "AGGREGATES" } )
public class MemoryGroupByMeta extends BaseGroupByMeta {
  private static Class<?> PKG = MemoryGroupByMeta.class; // for i18n purposes, needed by Translator2!!

  public static final int TYPE_GROUP_NONE = 0;

  public static final int TYPE_GROUP_SUM = 1;

  public static final int TYPE_GROUP_AVERAGE = 2;

  public static final int TYPE_GROUP_MEDIAN = 3;

  public static final int TYPE_GROUP_PERCENTILE = 4;

  public static final int TYPE_GROUP_MIN = 5;

  public static final int TYPE_GROUP_MAX = 6;

  public static final int TYPE_GROUP_COUNT_ALL = 7;

  public static final int TYPE_GROUP_CONCAT_COMMA = 8;

  public static final int TYPE_GROUP_FIRST = 9;

  public static final int TYPE_GROUP_LAST = 10;

  public static final int TYPE_GROUP_FIRST_INCL_NULL = 11;

  public static final int TYPE_GROUP_LAST_INCL_NULL = 12;

  public static final int TYPE_GROUP_STANDARD_DEVIATION = 13;

  public static final int TYPE_GROUP_CONCAT_STRING = 14;

  public static final int TYPE_GROUP_COUNT_DISTINCT = 15;

  public static final int TYPE_GROUP_COUNT_ANY = 16;

  public static final String[] typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */
  {
    "-", "SUM", "AVERAGE", "MEDIAN", "PERCENTILE", "MIN", "MAX", "COUNT_ALL", "CONCAT_COMMA", "FIRST", "LAST",
    "FIRST_INCL_NULL", "LAST_INCL_NULL", "STD_DEV", "CONCAT_STRING", "COUNT_DISTINCT", "COUNT_ANY", };

  public static final String[] typeGroupLongDesc = {
    "-", BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.SUM" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.AVERAGE" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MEDIAN" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.PERCENTILE" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MIN" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.MAX" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_ALL" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.CONCAT_STRING" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" ),
    BaseMessages.getString( PKG, "MemoryGroupByMeta.TypeGroupLongDesc.COUNT_ANY" ), };

  public MemoryGroupByMeta() {
    super(); // allocate BaseStepMeta
  }

  @Override
  protected void readData( Node stepnode ) throws KettleXMLException {
    try {
      super.readData( stepnode );
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
        PKG, "MemoryGroupByMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
    }
  }

  @Override
  public void getFields( RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) {
    // Check compatibility mode
    boolean compatibilityMode = ValueMetaBase.convertStringToBoolean(
      space.getVariable( Const.KETTLE_COMPATIBILITY_MEMORY_GROUP_BY_SUM_AVERAGE_RETURN_NUMBER_TYPE, "N" ) );

    // re-assemble a new row of metadata
    //
    RowMetaInterface fields = new RowMeta();

    // Add the grouping fields in the correct order...
    //
    for ( int i = 0; i < groupField.length; i++ ) {
      ValueMetaInterface valueMeta = r.searchValueMeta( groupField[i] );
      if ( valueMeta != null ) {
        valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
        fields.addValueMeta( valueMeta );
      }
    }

    // Re-add aggregates
    //
    for ( int i = 0; i < subjectField.length; i++ ) {
      ValueMetaInterface subj = r.searchValueMeta( subjectField[i] );
      if ( subj != null || aggregateType[i] == TYPE_GROUP_COUNT_ANY ) {
        String value_name = aggregateField[i];
        int value_type = ValueMetaInterface.TYPE_NONE;
        int length = -1;
        int precision = -1;

        switch ( aggregateType[i] ) {
          case TYPE_GROUP_FIRST:
          case TYPE_GROUP_LAST:
          case TYPE_GROUP_FIRST_INCL_NULL:
          case TYPE_GROUP_LAST_INCL_NULL:
          case TYPE_GROUP_MIN:
          case TYPE_GROUP_MAX:
            value_type = subj.getType();
            break;
          case TYPE_GROUP_COUNT_DISTINCT:
          case TYPE_GROUP_COUNT_ALL:
          case TYPE_GROUP_COUNT_ANY:
            value_type = ValueMetaInterface.TYPE_INTEGER;
            break;
          case TYPE_GROUP_CONCAT_COMMA:
            value_type = ValueMetaInterface.TYPE_STRING;
            break;
          case TYPE_GROUP_SUM:
          case TYPE_GROUP_AVERAGE:
            if ( !compatibilityMode && subj.isNumeric() ) {
              value_type = subj.getType();
            } else {
              value_type = ValueMetaInterface.TYPE_NUMBER;
            }
            break;
          case TYPE_GROUP_MEDIAN:
          case TYPE_GROUP_PERCENTILE:
          case TYPE_GROUP_STANDARD_DEVIATION:
            value_type = ValueMetaInterface.TYPE_NUMBER;
            break;
          case TYPE_GROUP_CONCAT_STRING:
            value_type = ValueMetaInterface.TYPE_STRING;
            break;
          default:
            break;
        }

        if ( aggregateType[i] == TYPE_GROUP_COUNT_ALL
          || aggregateType[i] == TYPE_GROUP_COUNT_DISTINCT || aggregateType[i] == TYPE_GROUP_COUNT_ANY ) {
          length = ValueMetaInterface.DEFAULT_INTEGER_LENGTH;
          precision = 0;
        } else if ( aggregateType[i] == TYPE_GROUP_SUM
          && value_type != ValueMetaInterface.TYPE_INTEGER && value_type != ValueMetaInterface.TYPE_NUMBER
          && value_type != ValueMetaInterface.TYPE_BIGNUMBER ) {
          // If it ain't numeric, we change it to Number
          //
          value_type = ValueMetaInterface.TYPE_NUMBER;
          precision = -1;
          length = -1;
        }

        if ( value_type != ValueMetaInterface.TYPE_NONE ) {
          ValueMetaInterface v;
          try {
            v = ValueMetaFactory.createValueMeta( value_name, value_type );
          } catch ( KettlePluginException e ) {
            log.logError(
              BaseMessages.getString( PKG, "MemoryGroupByMeta.Exception.UnknownValueMetaType" ), value_type, e );
            v = new ValueMetaNone( value_name );
          }
          v.setOrigin( origin );
          v.setLength( length, precision );

          if ( subj != null ) {
            v.setConversionMask( subj.getConversionMask() );
          }

          fields.addValueMeta( v );
        }
      }
    }

    // Now that we have all the fields we want, we should clear the original row and replace the values...
    //
    r.clear();
    r.addRowMeta( fields );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "give_back_row", alwaysGivingBackOneRow ) );

    retval.append( "      <group>" ).append( Const.CR );
    for ( int i = 0; i < groupField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "name", groupField[i] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </group>" ).append( Const.CR );

    retval.append( "      <fields>" ).append( Const.CR );
    for ( int i = 0; i < subjectField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "aggregate", aggregateField[i] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "subject", subjectField[i] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "type", getTypeDesc( aggregateType[i] ) ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "valuefield", valueField[i] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {
    try {
      super.readRep( rep, metaStore, id_step, databases );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "MemoryGroupByMeta.Exception.UnexpectedErrorInReadingStepInfoFromRepository" ), e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "give_back_row", alwaysGivingBackOneRow );

      for ( int i = 0; i < groupField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "group_name", groupField[i] );
      }

      for ( int i = 0; i < subjectField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_name", aggregateField[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_subject", subjectField[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_type", getTypeDesc( aggregateType[i] ) );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_value_field", valueField[i] );
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "MemoryGroupByMeta.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new MemoryGroupBy( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MemoryGroupByData();
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = ( subjectField == null ? -1 : subjectField.length );
    if ( nrFields <= 0 ) {
      return;
    }
    String[][] normalizedStringArrays = Utils.normalizeArrays( nrFields, aggregateField, valueField );
    aggregateField = normalizedStringArrays[ 0 ];
    valueField = normalizedStringArrays[ 1 ];

    int[][] normalizedIntArrays = Utils.normalizeArrays( nrFields, aggregateType );
    aggregateType = normalizedIntArrays[ 0 ];
  }
}
