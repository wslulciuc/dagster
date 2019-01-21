def get_function_name(context):
    return '{run_id}_function'.format(run_id=context.run_id)


def get_step_key(context, step_idx):
    return '{run_id}_step_{step_idx}.pickle'.format(run_id=context.run_id, step_idx=step_idx)


def get_input_key(context, step_idx):
    return '{run_id}_intermediate_results_{step_idx}.pickle'.format(
        run_id=context.run_id, step_idx=step_idx
    )


def get_deployment_package_key(context):
    return '{run_id}_deployment_package.zip'.format(run_id=context.run_id)


def get_resources_key(context):
    return '{run_id}_resources.pickle'.format(run_id=context.run_id)
