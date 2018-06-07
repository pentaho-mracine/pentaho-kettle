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

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Created on 02-jun-2003
 */
@InjectionSupported( localizationPrefix = "GroupByMeta.Injection" )
public class GroupByMeta extends BaseGroupByMeta {
  private static Class<?> PKG = GroupByMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * All rows need to pass, adding an extra row at the end of each group/block.
   */
  @Injection( name = "PASS_ALL_ROWS" )
  private boolean passAllRows;

  /**
   * Directory to store the temp files
   */
  @Injection( name = "TEMP_DIRECTORY" )
  private String directory;

  /**
   * Temp files prefix...
   */
  @Injection( name = "TEMP_FILE_PREFIX" )
  private String prefix;

  /**
   * Indicate that some rows don't need to be considered
   * TODO: make work in GUI & worker & add injection support
   */
  private boolean aggregateIgnored;

  /**
   * name of the boolean field that indicates we need to ignore the row
   * TODO: make work in GUI & worker & add injection support
   */
  private String aggregateIgnoredField;

  /**
   * Add a linenr in the group, resetting to 0 in a new group.
   */
  @Injection( name = "GROUP_LINE_NUMBER_ENABLED" )
  private boolean addingLineNrInGroup;

  /**
   * The fieldname that will contain the added integer field
   */
  @Injection( name = "GROUP_LINE_NUMBER_FIELDNAME" )
  private String lineNrInGroupField;

  @Override
  @Injection( name = "AGG_FIELDNAME" )
  public void setAggregateField( String[] aggregateField ) {
    this.aggregateField = aggregateField;
  }

  @Override
  @Injection( name = "AGG_TYPE" )
  public void setAggregateType( String[] aggregateType ) {
    this.aggregateType = aggregateType;
  }

  @Override
  @Injection( name = "GROUP_FIELDNAME" )
  public void setGroupField( String[] groupField ) {
    this.groupField = groupField;
  }

  @Override
  @Injection( name = "AGG_SUBJECT" )
  public void setSubjectField( String[] subjectField ) {
    this.subjectField = subjectField;
  }

  @Override
  @Injection( name = "AGG_VALUE" )
  public void setValueField( String[] valueField ) {
    this.valueField = valueField;
  }

  @Override
  @Injection( name = "ALLWAYS_PASS_A_ROW" )
  public void setAlwaysGivingBackOneRow( boolean alwaysGivingBackOneRow ) {
    this.alwaysGivingBackOneRow = alwaysGivingBackOneRow;
  }

  public GroupByMeta() {
    super(); // allocate BaseStepMeta
    aggregateFunctions = new GroupByType[]{
        GroupByType.NONE,
        GroupByType.SUM,
        GroupByType.AVERAGE,
        GroupByType.MEDIAN,
        GroupByType.PERCENTILE,
        GroupByType.MIN,
        GroupByType.MAX,
        GroupByType.COUNT_ALL,
        GroupByType.CONCAT_COMMA,
        GroupByType.FIRST,
        GroupByType.LAST,
        GroupByType.FIRST_INCL_NULL,
        GroupByType.LAST_INCL_NULL,
        GroupByType.CUMULATIVE_SUM,
        GroupByType.CUMULATIVE_AVERAGE,
        GroupByType.STANDARD_DEVIATION,
        GroupByType.CONCAT_STRING,
        GroupByType.COUNT_DISTINCT,
        GroupByType.COUNT_ANY,
        GroupByType.STANDARD_DEVIATION_SAMPLE,
        GroupByType.PERCENTILE_NEAREST_RANK
    };
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
    super.getFields( rowMeta, origin, info, nextStep, space, repository, metaStore, passAllRows );

    RowMetaInterface fields = new RowMeta();

    if ( passAllRows ) {
      // If we pass all rows, we can add a line nr in the group...
      if ( addingLineNrInGroup && !Utils.isEmpty( lineNrInGroupField ) ) {
        ValueMetaInterface lineNr = new ValueMetaInteger( lineNrInGroupField );
        lineNr.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH, 0 );
        lineNr.setOrigin( origin );
        fields.addValueMeta( lineNr );
      }
    }

    // This will just append the lineNr value to the end
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

    return super.getXML();
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
      super.saveRep( rep, metaStore, id_transformation, id_step );
      rep.saveStepAttribute( id_transformation, id_step, "all_rows", passAllRows );
      rep.saveStepAttribute( id_transformation, id_step, "ignore_aggregate", aggregateIgnored );
      rep.saveStepAttribute( id_transformation, id_step, "field_ignore", aggregateIgnoredField );
      rep.saveStepAttribute( id_transformation, id_step, "directory", directory );
      rep.saveStepAttribute( id_transformation, id_step, "prefix", prefix );
      rep.saveStepAttribute( id_transformation, id_step, "add_linenr", addingLineNrInGroup );
      rep.saveStepAttribute( id_transformation, id_step, "linenr_fieldname", lineNrInGroupField );
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
  public TransMeta.TransformationType[] getSupportedTransformationTypes() {
    return new TransMeta.TransformationType[] { TransMeta.TransformationType.Normal };
  }
}
