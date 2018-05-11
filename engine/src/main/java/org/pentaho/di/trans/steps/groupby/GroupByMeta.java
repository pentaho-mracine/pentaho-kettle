/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.groupby;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaInteger;
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
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Created on 02-jun-2003
 */
public class GroupByMeta extends BaseGroupByMeta {
  private static Class<?> PKG = GroupByMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String[] typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */
  {
    "-", "SUM", "AVERAGE", "MEDIAN", "PERCENTILE", "MIN", "MAX", "COUNT_ALL", "CONCAT_COMMA", "FIRST", "LAST",
    "FIRST_INCL_NULL", "LAST_INCL_NULL", "CUM_SUM", "CUM_AVG", "STD_DEV", "CONCAT_STRING", "COUNT_DISTINCT",
    "COUNT_ANY", "STD_DEV_SAMPLE", "PERCENTILE_NEAREST_RANK" };

  public static final String[] typeGroupLongDesc = {
    "-", BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.SUM" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MIN" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MAX" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_ALL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_SUM" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_AVERAGE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK" )
  };

  /**
   * All rows need to pass, adding an extra row at the end of each group/block.
   */
  private boolean passAllRows;

  /**
   * Directory to store the temp files
   */
  private String directory;

  /**
   * Temp files prefix...
   */
  private String prefix;

  /**
   * Indicate that some rows don't need to be considered : TODO: make work in GUI & worker
   */
  private boolean aggregateIgnored;

  /**
   * name of the boolean field that indicates we need to ignore the row : TODO: make work in GUI & worker
   */
  private String aggregateIgnoredField;

  /**
   * Add a linenr in the group, resetting to 0 in a new group.
   */
  private boolean addingLineNrInGroup;

  /**
   * The fieldname that will contain the added integer field
   */
  private String lineNrInGroupField;

  public GroupByMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the passAllRows.
   */
  public boolean passAllRows() {
    return passAllRows;
  }

  /**
   * @param passAllRows The passAllRows to set.
   */
  public void setPassAllRows( boolean passAllRows ) {
    this.passAllRows = passAllRows;
  }

  @Override
  protected void readData( Node stepnode ) throws KettleXMLException {
    try {
      super.readData( stepnode );

      passAllRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "all_rows" ) );
      aggregateIgnored = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "ignore_aggregate" ) );
      aggregateIgnoredField = XMLHandler.getTagValue( stepnode, "field_ignore" );

      directory = XMLHandler.getTagValue( stepnode, "directory" );
      prefix = XMLHandler.getTagValue( stepnode, "prefix" );

      addingLineNrInGroup = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "add_linenr" ) );
      lineNrInGroupField = XMLHandler.getTagValue( stepnode, "linenr_fieldname" );
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
        PKG, "GroupByMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    directory = "%%java.io.tmpdir%%";
    prefix = "grp";

    passAllRows = false;
    aggregateIgnored = false;
    aggregateIgnoredField = null;

    super.setDefault();
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) {
    // re-assemble a new row of metadata
    //
    RowMetaInterface fields = new RowMeta();

    if ( !passAllRows ) {
      // Add the grouping fields in the correct order...
      //
      for ( int i = 0; i < groupField.length; i++ ) {
        ValueMetaInterface valueMeta = rowMeta.searchValueMeta( groupField[ i ] );
        if ( valueMeta != null ) {
          fields.addValueMeta( valueMeta );
        }
      }
    } else {
      // Add all the original fields from the incoming row meta
      //
      fields.addRowMeta( rowMeta );
    }

    // Re-add aggregates
    //
    for ( int i = 0; i < subjectField.length; i++ ) {
      ValueMetaInterface subj = rowMeta.searchValueMeta( subjectField[ i ] );
      if ( subj != null || aggregateType[ i ] == TYPE_GROUP_COUNT_ANY ) {
        String valueName = aggregateField[ i ];
        int valueType = ValueMetaInterface.TYPE_NONE;
        int length = -1;
        int precision = -1;

        switch ( aggregateType[ i ] ) {
          case TYPE_GROUP_SUM:
          case TYPE_GROUP_AVERAGE:
          case TYPE_GROUP_CUMULATIVE_SUM:
          case TYPE_GROUP_CUMULATIVE_AVERAGE:
          case TYPE_GROUP_FIRST:
          case TYPE_GROUP_LAST:
          case TYPE_GROUP_FIRST_INCL_NULL:
          case TYPE_GROUP_LAST_INCL_NULL:
          case TYPE_GROUP_MIN:
          case TYPE_GROUP_MAX:
            valueType = subj.getType();
            break;
          case TYPE_GROUP_COUNT_DISTINCT:
          case TYPE_GROUP_COUNT_ANY:
          case TYPE_GROUP_COUNT_ALL:
            valueType = ValueMetaInterface.TYPE_INTEGER;
            break;
          case TYPE_GROUP_CONCAT_COMMA:
            valueType = ValueMetaInterface.TYPE_STRING;
            break;
          case TYPE_GROUP_STANDARD_DEVIATION:
          case TYPE_GROUP_MEDIAN:
          case TYPE_GROUP_STANDARD_DEVIATION_SAMPLE:
          case TYPE_GROUP_PERCENTILE:
          case TYPE_GROUP_PERCENTILE_NEAREST_RANK:
            valueType = ValueMetaInterface.TYPE_NUMBER;
            break;
          case TYPE_GROUP_CONCAT_STRING:
            valueType = ValueMetaInterface.TYPE_STRING;
            break;
          default:
            break;
        }

        // Change type from integer to number in case off averages for cumulative average
        //
        if ( aggregateType[ i ] == TYPE_GROUP_CUMULATIVE_AVERAGE && valueType == ValueMetaInterface.TYPE_INTEGER ) {
          valueType = ValueMetaInterface.TYPE_NUMBER;
          precision = -1;
          length = -1;
        } else if ( aggregateType[ i ] == TYPE_GROUP_COUNT_ALL
            || aggregateType[ i ] == TYPE_GROUP_COUNT_DISTINCT || aggregateType[ i ] == TYPE_GROUP_COUNT_ANY ) {
          length = ValueMetaInterface.DEFAULT_INTEGER_LENGTH;
          precision = 0;
        } else if ( aggregateType[ i ] == TYPE_GROUP_SUM
            && valueType != ValueMetaInterface.TYPE_INTEGER && valueType != ValueMetaInterface.TYPE_NUMBER
            && valueType != ValueMetaInterface.TYPE_BIGNUMBER ) {
          // If it ain't numeric, we change it to Number
          //
          valueType = ValueMetaInterface.TYPE_NUMBER;
          precision = -1;
          length = -1;
        }

        if ( valueType != ValueMetaInterface.TYPE_NONE ) {
          ValueMetaInterface v;
          try {
            v = ValueMetaFactory.createValueMeta( valueName, valueType );
          } catch ( KettlePluginException e ) {
            v = new ValueMetaNone( valueName );
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

    if ( passAllRows ) {
      // If we pass all rows, we can add a line nr in the group...
      if ( addingLineNrInGroup && !Utils.isEmpty( lineNrInGroupField ) ) {
        ValueMetaInterface lineNr = new ValueMetaInteger( lineNrInGroupField );
        lineNr.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
        lineNr.setOrigin( origin );
        fields.addValueMeta( lineNr );
      }

    }

    // Now that we have all the fields we want, we should clear the original row and replace the values...
    //
    rowMeta.clear();
    rowMeta.addRowMeta( fields );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "all_rows", passAllRows ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "ignore_aggregate", aggregateIgnored ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "field_ignore", aggregateIgnoredField ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "directory", directory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "prefix", prefix ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_linenr", addingLineNrInGroup ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "linenr_fieldname", lineNrInGroupField ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "give_back_row", alwaysGivingBackOneRow ) );

    retval.append( "      <group>" ).append( Const.CR );
    for ( int i = 0; i < groupField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "name", groupField[ i ] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </group>" ).append( Const.CR );

    retval.append( "      <fields>" ).append( Const.CR );
    for ( int i = 0; i < subjectField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "aggregate", aggregateField[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "subject", subjectField[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "type", getTypeDesc( aggregateType[ i ] ) ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "valuefield", valueField[ i ] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      passAllRows = rep.getStepAttributeBoolean( id_step, "all_rows" );
      aggregateIgnored = rep.getStepAttributeBoolean( id_step, "ignore_aggregate" );
      aggregateIgnoredField = rep.getStepAttributeString( id_step, "field_ignore" );
      directory = rep.getStepAttributeString( id_step, "directory" );
      prefix = rep.getStepAttributeString( id_step, "prefix" );
      addingLineNrInGroup = rep.getStepAttributeBoolean( id_step, "add_linenr" );
      lineNrInGroupField = rep.getStepAttributeString( id_step, "linenr_fieldname" );

      super.readRep( rep, metaStore, id_step, databases );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "GroupByMeta.Exception.UnexpectedErrorInReadingStepInfoFromRepository" ), e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "all_rows", passAllRows );
      rep.saveStepAttribute( id_transformation, id_step, "ignore_aggregate", aggregateIgnored );
      rep.saveStepAttribute( id_transformation, id_step, "field_ignore", aggregateIgnoredField );
      rep.saveStepAttribute( id_transformation, id_step, "directory", directory );
      rep.saveStepAttribute( id_transformation, id_step, "prefix", prefix );
      rep.saveStepAttribute( id_transformation, id_step, "add_linenr", addingLineNrInGroup );
      rep.saveStepAttribute( id_transformation, id_step, "linenr_fieldname", lineNrInGroupField );
      rep.saveStepAttribute( id_transformation, id_step, "give_back_row", alwaysGivingBackOneRow );

      for ( int i = 0; i < groupField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "group_name", groupField[ i ] );
      }

      for ( int i = 0; i < subjectField.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_name", aggregateField[ i ] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_subject", subjectField[ i ] );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_type", getTypeDesc( aggregateType[ i ] ) );
        rep.saveStepAttribute( id_transformation, id_step, i, "aggregate_value_field", valueField[ i ] );
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "GroupByMeta.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new GroupBy( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new GroupByData();
  }

  /**
   * @return Returns the directory.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  /**
   * @return Returns the prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param prefix The prefix to set.
   */
  public void setPrefix( String prefix ) {
    this.prefix = prefix;
  }

  /**
   * @return the addingLineNrInGroup
   */
  public boolean isAddingLineNrInGroup() {
    return addingLineNrInGroup;
  }

  /**
   * @param addingLineNrInGroup the addingLineNrInGroup to set
   */
  public void setAddingLineNrInGroup( boolean addingLineNrInGroup ) {
    this.addingLineNrInGroup = addingLineNrInGroup;
  }

  /**
   * @return the lineNrInGroupField
   */
  public String getLineNrInGroupField() {
    return lineNrInGroupField;
  }

  /**
   * @param lineNrInGroupField the lineNrInGroupField to set
   */
  public void setLineNrInGroupField( String lineNrInGroupField ) {
    this.lineNrInGroupField = lineNrInGroupField;
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new GroupByMetaInjection( this );
  }

  @Override
  public TransMeta.TransformationType[] getSupportedTransformationTypes() {
    return new TransMeta.TransformationType[] { TransMeta.TransformationType.Normal };
  }
}
