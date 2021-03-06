import { APP_QUERY } from "../App";
import { TYPE_EXPLORER_CONTAINER_QUERY } from "../typeexplorer/TypeExplorerContainer";
import { TYPE_LIST_CONTAINER_QUERY } from "../typeexplorer/TypeListContainer";

const MOCKS = [
  {
    request: {
      operationName: "PipelineseContainerQuery",
      queryVariableName: "APP_QUERY",
      query: APP_QUERY,
      variables: undefined
    },
    result: {
      data: {
        pipelinesOrError: {
          __typename: "PipelineConnection",
          nodes: [
            {
              name: "input_transform_test_pipeline",
              description: null,
              contexts: [
                {
                  name: "default",
                  description: null,
                  __typename: "PipelineContext",
                  config: {
                    configType: {
                      key: "Dict.32",
                      name: null,
                      description:
                        "A configuration dictionary with typed fields",
                      isList: false,
                      isNullable: false,
                      isSelector: false,
                      innerTypes: [
                        {
                          key: "String",
                          __typename: "RegularConfigType",
                          name: "String",
                          description: "",
                          isList: false,
                          isNullable: false,
                          isSelector: false,
                          innerTypes: []
                        }
                      ],
                      fields: [
                        {
                          name: "log_level",
                          description: null,
                          isOptional: true,
                          configType: {
                            key: "String",
                            __typename: "RegularConfigType"
                          },
                          __typename: "ConfigTypeField"
                        }
                      ],
                      __typename: "CompositeConfigType"
                    },
                    __typename: "ConfigTypeField"
                  },
                  resources: []
                }
              ],
              solids: [
                {
                  name: "pandas_source_test",
                  definition: {
                    metadata: [
                      {
                        key: "notebook_path",
                        value:
                          "/Users/bengotow/Work/F376/Projects/dagster/python_modules/dagster-pandas/dagster_pandas/examples/notebooks/pandas_source_test.ipynb",
                        __typename: "SolidMetadataItemDefinition"
                      },
                      {
                        key: "kind",
                        value: "ipynb",
                        __typename: "SolidMetadataItemDefinition"
                      }
                    ],
                    configDefinition: null,
                    __typename: "SolidDefinition",
                    description:
                      "This solid is backed by the notebook at /Users/bengotow/Work/F376/Projects/dagster/python_modules/dagster-pandas/dagster_pandas/examples/notebooks/pandas_source_test.ipynb"
                  },
                  inputs: [
                    {
                      definition: {
                        name: "df",
                        type: {
                          name: "PandasDataFrame",
                          displayName: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        __typename: "InputDefinition",
                        description: null,
                        expectations: []
                      },
                      dependsOn: {
                        definition: {
                          name: "result",
                          __typename: "OutputDefinition"
                        },
                        solid: { name: "load_num_csv", __typename: "Solid" },
                        __typename: "Output"
                      },
                      __typename: "Input"
                    }
                  ],
                  outputs: [
                    {
                      definition: {
                        name: "result",
                        type: {
                          name: "PandasDataFrame",
                          displayName: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        expectations: [],
                        __typename: "OutputDefinition",
                        description: null
                      },
                      dependedBy: [],
                      __typename: "Output"
                    }
                  ],
                  __typename: "Solid"
                },
                {
                  name: "load_num_csv",
                  definition: {
                    metadata: [],
                    configDefinition: null,
                    __typename: "SolidDefinition",
                    description: null
                  },
                  inputs: [],
                  outputs: [
                    {
                      definition: {
                        name: "result",
                        type: {
                          name: "Any",
                          displayName: "Any",
                          __typename: "RegularRuntimeType",
                          description: null
                        },
                        expectations: [],
                        __typename: "OutputDefinition",
                        description: null
                      },
                      dependedBy: [
                        {
                          solid: {
                            name: "pandas_source_test",
                            __typename: "Solid"
                          },
                          __typename: "Input"
                        }
                      ],
                      __typename: "Output"
                    }
                  ],
                  __typename: "Solid"
                }
              ],
              __typename: "Pipeline",
              environmentType: {
                name: "InputTransformTestPipeline.Environment",
                __typename: "CompositeConfigType"
              }
            },
            {
              name: "pandas_hello_world",
              description: null,
              contexts: [
                {
                  name: "default",
                  description: null,
                  __typename: "PipelineContext",
                  config: {
                    configType: {
                      key: "Dict.29",
                      name: null,
                      description:
                        "A configuration dictionary with typed fields",
                      isList: false,
                      isNullable: false,
                      isSelector: false,
                      innerTypes: [
                        {
                          key: "String",
                          __typename: "RegularConfigType",
                          name: "String",
                          description: "",
                          isList: false,
                          isNullable: false,
                          isSelector: false,
                          innerTypes: []
                        }
                      ],
                      fields: [
                        {
                          name: "log_level",
                          description: null,
                          isOptional: true,
                          configType: {
                            key: "String",
                            __typename: "RegularConfigType"
                          },
                          __typename: "ConfigTypeField"
                        }
                      ],
                      __typename: "CompositeConfigType"
                    },
                    __typename: "ConfigTypeField"
                  },
                  resources: []
                }
              ],
              solids: [
                {
                  name: "sum_solid",
                  definition: {
                    metadata: [],
                    configDefinition: null,
                    __typename: "SolidDefinition",
                    description: null
                  },
                  inputs: [
                    {
                      definition: {
                        name: "num",
                        type: {
                          name: "PandasDataFrame",
                          displayName: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        __typename: "InputDefinition",
                        description: null,
                        expectations: []
                      },
                      dependsOn: null,
                      __typename: "Input"
                    }
                  ],
                  outputs: [
                    {
                      definition: {
                        name: "result",
                        type: {
                          name: "PandasDataFrame",
                          displayName: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        expectations: [],
                        __typename: "OutputDefinition",
                        description: null
                      },
                      dependedBy: [
                        {
                          solid: { name: "sum_sq_solid", __typename: "Solid" },
                          __typename: "Input"
                        }
                      ],
                      __typename: "Output"
                    }
                  ],
                  __typename: "Solid"
                },
                {
                  name: "sum_sq_solid",
                  definition: {
                    metadata: [],
                    configDefinition: null,
                    __typename: "SolidDefinition",
                    description: null
                  },
                  inputs: [
                    {
                      definition: {
                        name: "sum_df",
                        type: {
                          name: "PandasDataFrame",
                          displayName: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        __typename: "InputDefinition",
                        description: null,
                        expectations: []
                      },
                      dependsOn: {
                        definition: {
                          name: "result",
                          __typename: "OutputDefinition"
                        },
                        solid: { name: "sum_solid", __typename: "Solid" },
                        __typename: "Output"
                      },
                      __typename: "Input"
                    }
                  ],
                  outputs: [
                    {
                      definition: {
                        name: "result",
                        type: {
                          displayName: "PandasDataFrame",
                          name: "PandasDataFrame",
                          __typename: "RegularRuntimeType",
                          description:
                            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/"
                        },
                        expectations: [],
                        __typename: "OutputDefinition",
                        description: null
                      },
                      dependedBy: [],
                      __typename: "Output"
                    }
                  ],
                  __typename: "Solid"
                }
              ],
              __typename: "Pipeline",
              environmentType: {
                name: "PandasHelloWorld.Environment",
                __typename: "CompositeConfigType"
              }
            }
          ]
        }
      }
    }
  },

  {
    request: {
      operationName: "TypeExplorerContainerQuery",
      queryVariableName: "TYPE_EXPLORER_CONTAINER_QUERY",
      query: TYPE_EXPLORER_CONTAINER_QUERY,
      variables: {
        pipelineName: "pandas_hello_world",
        runtimeTypeName: "PandasDataFrame"
      }
    },
    result: {
      data: {
        runtimeTypeOrError: {
          __typename: "RegularRuntimeType",
          displayName: "PandasDataFrame",
          name: "PandasDataFrame",
          description:
            "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/",
          inputSchemaType: {
            key: "DataFrameInputSchema",
            name: "DataFrameInputSchema",
            description: null,
            isList: false,
            isNullable: false,
            isSelector: true,
            innerTypes: [
              {
                key: "Path",
                __typename: "RegularConfigType",
                name: "Path",
                description: "",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: []
              },
              {
                key: "String",
                __typename: "RegularConfigType",
                name: "String",
                description: "",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: []
              },
              {
                key: "Dict.28",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [{ key: "Path", __typename: "RegularConfigType" }],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              },
              {
                key: "Dict.26",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [
                  { key: "String", __typename: "RegularConfigType" },
                  { key: "Path", __typename: "RegularConfigType" }
                ],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  },
                  {
                    name: "sep",
                    description: null,
                    isOptional: true,
                    configType: {
                      key: "String",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              },
              {
                key: "Dict.27",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [{ key: "Path", __typename: "RegularConfigType" }],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              }
            ],
            fields: [
              {
                name: "csv",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.26",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              },
              {
                name: "parquet",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.27",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              },
              {
                name: "table",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.28",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              }
            ],
            __typename: "CompositeConfigType"
          },
          outputSchemaType: {
            key: "DataFrameOutputSchema",
            name: "DataFrameOutputSchema",
            description: null,
            isList: false,
            isNullable: false,
            isSelector: true,
            innerTypes: [
              {
                key: "Dict.23",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [
                  { key: "String", __typename: "RegularConfigType" },
                  { key: "Path", __typename: "RegularConfigType" }
                ],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  },
                  {
                    name: "sep",
                    description: null,
                    isOptional: true,
                    configType: {
                      key: "String",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              },
              {
                key: "Path",
                __typename: "RegularConfigType",
                name: "Path",
                description: "",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: []
              },
              {
                key: "String",
                __typename: "RegularConfigType",
                name: "String",
                description: "",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: []
              },
              {
                key: "Dict.24",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [{ key: "Path", __typename: "RegularConfigType" }],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              },
              {
                key: "Dict.25",
                __typename: "CompositeConfigType",
                name: null,
                description: "A configuration dictionary with typed fields",
                isList: false,
                isNullable: false,
                isSelector: false,
                innerTypes: [{ key: "Path", __typename: "RegularConfigType" }],
                fields: [
                  {
                    name: "path",
                    description: null,
                    isOptional: false,
                    configType: {
                      key: "Path",
                      __typename: "RegularConfigType"
                    },
                    __typename: "ConfigTypeField"
                  }
                ]
              }
            ],
            fields: [
              {
                name: "csv",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.23",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              },
              {
                name: "parquet",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.24",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              },
              {
                name: "table",
                description: null,
                isOptional: false,
                configType: {
                  key: "Dict.25",
                  __typename: "CompositeConfigType"
                },
                __typename: "ConfigTypeField"
              }
            ],
            __typename: "CompositeConfigType"
          }
        }
      }
    }
  },

  {
    request: {
      operationName: "TypeListContainerQuery",
      queryVariableName: "TYPE_LIST_CONTAINER_QUERY",
      query: TYPE_LIST_CONTAINER_QUERY,
      variables: { pipelineName: "pandas_hello_world" }
    },
    result: {
      data: {
        pipelineOrError: {
          __typename: "Pipeline",
          runtimeTypes: [
            {
              name: "Any",
              displayName: "Any",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            },
            {
              name: "Bool",
              displayName: "Bool",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            },
            {
              name: "Float",
              displayName: "Float",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            },
            {
              name: "Int",
              displayName: "Int",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            },
            {
              name: "PandasDataFrame",
              displayName: "PandasDataFrame",
              isBuiltin: false,
              description:
                "Two-dimensional size-mutable, potentially heterogeneous\n    tabular data structure with labeled axes (rows and columns).\n    See http://pandas.pydata.org/",
              __typename: "RegularRuntimeType"
            },
            {
              name: "Path",
              displayName: "Path",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            },
            {
              name: "String",
              displayName: "String",
              isBuiltin: true,
              description: null,
              __typename: "RegularRuntimeType"
            }
          ]
        }
      }
    }
  }
];

export default MOCKS;
